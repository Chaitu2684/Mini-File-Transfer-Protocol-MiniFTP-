#!/usr/bin/env python3
"""Mini-FTP server over UDP with stop-and-wait reliability (Part 2).

The server owns a single well-known UDP port.  Clients are told apart by
the (IP, port) tuple that recvfrom() returns: the main loop reads every
datagram and routes it to a per-client session queue, while one worker
thread per session runs the transfer.  Datagrams are always sent from the
one bound socket, so no extra ports are opened.

Control messages (one ASCII datagram each):
    PUT|<filename>|<filesize>|<total_chunks>  -> READY|0 | ERROR|<reason>
    GET|<filename>|0                          -> READY|<filesize>|<chunks>
                                                 | ERROR|<reason>
    LIST                                      -> OK|<count>|n1,s1;n2,s2;...
    DONE|<md5>                                -> VERIFIED | MISMATCH

DATA datagram: struct "!IBH" (seq_num, is_last, data_len) + <= 1000 bytes.
ACK datagram : struct "!I"   (seq_num).

Usage:
    python3 UDPFTPServer.py <port> [--loss-rate 0.2]
"""

import argparse
import hashlib
import os
import queue
import random
import socket
import struct
import sys
import threading
import time

CHUNK_SIZE = 1000
HEADER_FORMAT = "!IBH"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)  # 7 bytes
ACK_FORMAT = "!I"
ACK_SIZE = struct.calcsize(ACK_FORMAT)        # 4 bytes
RECV_BUFFER = 2048
ACK_TIMEOUT = 0.5      # required default, see 2.4
MAX_RETRIES = 10       # consecutive timeouts before aborting a chunk
IDLE_TIMEOUT = 15.0    # receiver gives up if the peer goes silent
DONE_WAIT = 10.0       # how long the receiver waits for DONE|<md5>
STORAGE_DIR = "server_storage"

_print_lock = threading.Lock()


def log(message):
    """Prints a server log line atomically."""
    with _print_lock:
        print(message, flush=True)


def md5_of_file(path):
    """Returns the hex MD5 digest of a file."""
    digest = hashlib.md5()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 16), b""):
            digest.update(block)
    return digest.hexdigest()


def total_chunks_for(filesize):
    """Number of 1000-byte chunks; an empty file still costs one chunk."""
    if filesize <= 0:
        return 1
    return (filesize + CHUNK_SIZE - 1) // CHUNK_SIZE


def safe_storage_path(filename):
    """Maps a requested name onto a path inside ``server_storage/``."""
    if not filename or filename in (".", ".."):
        return None
    if "/" in filename or "\\" in filename or os.path.isabs(filename):
        return None
    if os.path.basename(filename) != filename:
        return None
    return os.path.join(STORAGE_DIR, filename)


class Transport:
    """Wraps the shared UDP socket and the optional loss simulator.

    ``loss_rate`` drops outgoing datagrams at the application layer.  It
    is a development aid only and must be 0 for the graded runs, where
    loss is injected with tc netem / NetLimiter instead.
    """

    def __init__(self, sock, loss_rate=0.1):
        self._sock = sock
        self._loss_rate = loss_rate
        self._lock = threading.Lock()

    def sendto(self, payload, address, label=""):
        """Sends a datagram unless the loss simulator swallows it."""
        if self._loss_rate > 0 and random.random() < self._loss_rate:
            log("[loss] dropped outgoing %s to %s:%d" % ((label,) + address))
            return False
        with self._lock:
            self._sock.sendto(payload, address)
        return True

    def send_control(self, text, address):
        """Sends one ASCII control datagram."""
        return self.sendto(text.encode("ascii"), address, label=text)


class Session:
    """Per-client state: an inbox of datagrams plus its worker thread."""

    def __init__(self, address):
        self.address = address
        self.inbox = queue.Queue()
        self.thread = None
        # Set once the transfer is over: a fresh control message from the
        # same client is then treated as a new request instead of being
        # queued for a session that is only lingering.
        self.finished = False


def is_control(packet):
    """True if the datagram looks like an ASCII control message."""
    return packet.startswith((b"PUT|", b"GET|", b"LIST", b"DONE|"))


def make_data_packet(seq_num, is_last, data):
    """Builds one DATA datagram: 7-byte header + payload."""
    header = struct.pack(HEADER_FORMAT, seq_num, 1 if is_last else 0,
                         len(data))
    return header + data


def parse_data_packet(packet):
    """Splits a DATA datagram into (seq_num, is_last, data) or None."""
    if len(packet) < HEADER_SIZE:
        return None
    seq_num, is_last, data_len = struct.unpack(HEADER_FORMAT,
                                               packet[:HEADER_SIZE])
    data = packet[HEADER_SIZE:HEADER_SIZE + data_len]
    if len(data) != data_len:
        return None
    return seq_num, is_last, data


def receive_file(transport, session, filename, filesize, total_chunks):
    """Server side of a PUT: acts as the stop-and-wait receiver."""
    address = session.address
    peer = "%s:%d" % address
    path = safe_storage_path(filename)
    temp_path = path + ".part"
    expected = 0
    duplicates = 0
    out_of_order = 0
    started = time.time()

    try:
        sink = open(temp_path, "wb")
    except OSError as error:
        transport.send_control("ERROR|cannot open file: %s" % error, address)
        log("[!] %s: cannot open %s (%s)" % (peer, temp_path, error))
        return

    try:
        while expected < total_chunks:
            try:
                packet = session.inbox.get(timeout=IDLE_TIMEOUT)
            except queue.Empty:
                log("[-] %s went silent after chunk %d; upload of %s aborted"
                    % (peer, expected - 1, filename))
                sink.close()
                _remove_quietly(temp_path)
                return

            if is_control(packet):
                if packet.startswith(b"PUT|"):
                    # Our READY was lost and the client asked again.
                    transport.send_control("READY|0", address)
                continue

            parsed = parse_data_packet(packet)
            if parsed is None:
                log("[!] %s sent a malformed datagram (%d bytes)"
                    % (peer, len(packet)))
                continue
            seq_num, is_last, data = parsed

            if seq_num == expected:
                sink.write(data)
                transport.sendto(struct.pack(ACK_FORMAT, seq_num), address,
                                 label="ACK %d" % seq_num)
                expected += 1
                if expected % 50 == 0 or is_last:
                    log("[+] %s -> %s: %d/%d chunks received"
                        % (peer, filename, expected, total_chunks))
                if is_last:
                    break
            elif seq_num < expected:
                duplicates += 1
                log("[dup] %s: chunk %d already stored, re-sending ACK %d"
                    % (peer, seq_num, seq_num))
                transport.sendto(struct.pack(ACK_FORMAT, seq_num), address,
                                 label="ACK %d" % seq_num)
            else:
                out_of_order += 1
                log("[ooo] %s: chunk %d arrived early (expecting %d), "
                    "discarded without ACK" % (peer, seq_num, expected))
    finally:
        if not sink.closed:
            sink.close()

    elapsed = time.time() - started
    received_bytes = os.path.getsize(temp_path)
    log("[+] %s: %s assembled (%d bytes, %.3f s, %d duplicate chunk(s), "
        "%d out-of-order)" % (peer, filename, received_bytes, elapsed,
                              duplicates, out_of_order))

    # Wait for DONE|<md5>, re-ACKing duplicate DATA in the meantime.
    verified = False
    done_seen = False
    deadline = time.time() + DONE_WAIT
    while time.time() < deadline:
        try:
            packet = session.inbox.get(timeout=0.5)
        except queue.Empty:
            if done_seen:
                break
            continue
        if packet.startswith(b"DONE|"):
            digest = packet.decode("ascii", "replace").split("|", 1)[1].strip()
            local_digest = md5_of_file(temp_path)
            verified = (digest == local_digest)
            transport.send_control("VERIFIED" if verified else "MISMATCH",
                                   address)
            log("[+] %s: MD5 %s (sender=%s, local=%s)"
                % (peer, "VERIFIED" if verified else "MISMATCH",
                   digest, local_digest))
            done_seen = True
            session.finished = True
            if verified:
                finalize(temp_path, path, filename, received_bytes)
            deadline = time.time() + 2.0  # linger for a retransmitted DONE
        elif not is_control(packet):
            parsed = parse_data_packet(packet)
            if parsed is not None:
                log("[dup] %s: chunk %d after completion, re-sending ACK"
                    % (peer, parsed[0]))
                transport.sendto(struct.pack(ACK_FORMAT, parsed[0]), address,
                                 label="ACK %d" % parsed[0])

    if not verified:
        log("[!] %s: %s not stored (no VERIFIED result)" % (peer, filename))
        _remove_quietly(temp_path)


def finalize(temp_path, path, filename, received_bytes):
    """Promotes the fully received temporary file to its final name."""
    try:
        os.replace(temp_path, path)
        log("[+] Saved %s (%d bytes)" % (filename, received_bytes))
    except OSError as error:
        log("[!] Could not store %s: %s" % (filename, error))
        _remove_quietly(temp_path)


def send_file(transport, session, filename, path, filesize, total_chunks):
    """Server side of a GET: acts as the stop-and-wait sender."""
    address = session.address
    peer = "%s:%d" % address
    retransmissions = 0
    started = time.time()

    with open(path, "rb") as source:
        for seq_num in range(total_chunks):
            data = source.read(CHUNK_SIZE)
            is_last = (seq_num == total_chunks - 1)
            packet = make_data_packet(seq_num, is_last, data)

            retries = 0
            acked = False
            while not acked:
                transport.sendto(packet, address, label="DATA %d" % seq_num)
                deadline = time.time() + ACK_TIMEOUT
                while True:
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        break
                    try:
                        reply = session.inbox.get(timeout=remaining)
                    except queue.Empty:
                        break
                    if len(reply) == ACK_SIZE and not is_control(reply):
                        ack = struct.unpack(ACK_FORMAT, reply)[0]
                        if ack == seq_num:
                            acked = True
                            break
                        # A stale ACK: keep waiting for the right one.
                    elif reply.startswith(b"GET|"):
                        transport.send_control(
                            "READY|%d|%d" % (filesize, total_chunks), address)
                if not acked:
                    retries += 1
                    retransmissions += 1
                    log("[rtx] %s: timeout on chunk %d, retry %d/%d"
                        % (peer, seq_num, retries, MAX_RETRIES))
                    if retries >= MAX_RETRIES:
                        log("Transfer failed: no ACK for chunk %d after %d "
                            "retries" % (seq_num, MAX_RETRIES))
                        return
            if (seq_num + 1) % 50 == 0 or is_last:
                log("[+] %s <- %s: %d/%d chunks sent"
                    % (peer, filename, seq_num + 1, total_chunks))

    elapsed = time.time() - started
    log("[+] %s: %s sent (%d bytes, %.3f s, %d retransmission(s), "
        "%.2f KB/s)" % (peer, filename, filesize, elapsed, retransmissions,
                        filesize / 1024.0 / max(elapsed, 1e-9)))

    # Announce the digest and wait for the receiver's verdict.
    done_message = "DONE|%s" % md5_of_file(path)
    for attempt in range(1, 6):
        transport.send_control(done_message, address)
        deadline = time.time() + ACK_TIMEOUT
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            try:
                reply = session.inbox.get(timeout=remaining)
            except queue.Empty:
                break
            if reply in (b"VERIFIED", b"MISMATCH"):
                log("[+] %s: receiver reported %s"
                    % (peer, reply.decode("ascii")))
                session.finished = True
                return
            if not is_control(reply) and len(reply) == ACK_SIZE:
                continue  # A late ACK for the final chunk.
        log("[rtx] %s: no verdict for DONE, retry %d/5" % (peer, attempt))
    log("[!] %s: never received a VERIFIED/MISMATCH verdict" % peer)


def handle_list(transport, address):
    """Answers LIST with a single datagram."""
    entries = []
    for name in sorted(os.listdir(STORAGE_DIR)):
        full = os.path.join(STORAGE_DIR, name)
        if os.path.isfile(full) and not name.endswith(".part"):
            entries.append("%s,%d" % (name, os.path.getsize(full)))
    transport.send_control("OK|%d|%s" % (len(entries), ";".join(entries)),
                           address)
    log("[+] %s:%d listed %d file(s)" % (address + (len(entries),)))


def start_session(transport, sessions, sessions_lock, address, target, args):
    """Registers a session and runs ``target`` in its own thread."""
    session = Session(address)
    with sessions_lock:
        sessions[address] = session

    def runner():
        try:
            target(transport, session, *args)
        except Exception as error:  # keep one bad transfer from killing all
            log("[!] %s:%d session error: %s" % (address + (error,)))
        finally:
            with sessions_lock:
                if sessions.get(address) is session:
                    del sessions[address]
            log("[-] %s:%d session closed" % address)

    session.thread = threading.Thread(target=runner, daemon=True)
    session.thread.start()


def handle_control(transport, sessions, sessions_lock, packet, address):
    """Handles the first datagram of a new exchange."""
    peer = "%s:%d" % address
    try:
        text = packet.decode("ascii").strip()
    except UnicodeDecodeError:
        log("[!] %s sent a stray binary datagram (no active session)" % peer)
        return

    fields = text.split("|")
    command = fields[0].upper()

    if command == "LIST":
        handle_list(transport, address)
        return

    if command == "PUT":
        if len(fields) < 4:
            transport.send_control("ERROR|malformed PUT", address)
            return
        filename = fields[1]
        try:
            filesize = int(fields[2])
            total_chunks = int(fields[3])
        except ValueError:
            transport.send_control("ERROR|malformed PUT", address)
            return
        if safe_storage_path(filename) is None:
            transport.send_control("ERROR|illegal filename", address)
            log("[!] %s tried an illegal filename: %r" % (peer, filename))
            return
        log("[+] %s uploading %s (%d bytes, %d chunks)"
            % (peer, filename, filesize, total_chunks))
        transport.send_control("READY|0", address)
        start_session(transport, sessions, sessions_lock, address,
                      receive_file, (filename, filesize, total_chunks))
        return

    if command == "GET":
        if len(fields) < 2:
            transport.send_control("ERROR|malformed GET", address)
            return
        filename = fields[1]
        path = safe_storage_path(filename)
        if path is None or not os.path.isfile(path):
            transport.send_control("ERROR|file not found", address)
            log("[!] %s requested a missing file: %r" % (peer, filename))
            return
        filesize = os.path.getsize(path)
        total_chunks = total_chunks_for(filesize)
        log("[+] %s downloading %s (%d bytes, %d chunks)"
            % (peer, filename, filesize, total_chunks))
        transport.send_control("READY|%d|%d" % (filesize, total_chunks),
                               address)
        start_session(transport, sessions, sessions_lock, address, send_file,
                      (filename, path, filesize, total_chunks))
        return

    log("[!] %s sent an unknown control message: %r" % (peer, text))
    transport.send_control("ERROR|unknown command", address)


def _remove_quietly(path):
    """Deletes a file, ignoring the case where it is already gone."""
    try:
        os.remove(path)
    except OSError:
        pass


def serve(port, loss_rate):
    """Runs the demultiplexing loop on the single well-known UDP port."""
    os.makedirs(STORAGE_DIR, exist_ok=True)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("", port))
    transport = Transport(sock, loss_rate)
    sessions = {}
    sessions_lock = threading.Lock()

    log("[*] Mini-FTP (UDP) server listening on port %d, storage=%s/"
        % (port, STORAGE_DIR))
    if loss_rate > 0:
        log("[*] Application-layer loss simulation active: %.0f%% "
            "(disable for graded runs)" % (loss_rate * 100))
    try:
        while True:
            packet, address = sock.recvfrom(RECV_BUFFER)
            with sessions_lock:
                session = sessions.get(address)
                if session is not None and session.finished and \
                        is_control(packet):
                    # The previous transfer is done; this is a new request.
                    del sessions[address]
                    session = None
            if session is not None:
                session.inbox.put(packet)
            else:
                handle_control(transport, sessions, sessions_lock, packet,
                               address)
    except KeyboardInterrupt:
        log("\n[*] Shutting down")
    finally:
        sock.close()


def main():
    parser = argparse.ArgumentParser(
        description="Mini-FTP server (UDP, stop-and-wait).")
    parser.add_argument("port", type=int, help="listening port (> 10000)")
    parser.add_argument("--loss-rate", type=float, default=0.0,
                        help="probability of dropping an outgoing datagram, "
                             "for local testing only (default 0.0)")
    args = parser.parse_args()
    if not 1024 < args.port < 65536:
        print("Please choose a port above 10000.", file=sys.stderr)
        sys.exit(1)
    serve(args.port, args.loss_rate)


if __name__ == "__main__":
    main()

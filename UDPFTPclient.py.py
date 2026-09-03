#!/usr/bin/env python3
"""Mini-FTP client over UDP with stop-and-wait reliability (Part 2).

Interactive commands:
    put <local_filename>    upload a file (client is the sender)
    get <remote_filename>   download a file (client is the receiver)
    list                    list the files the server holds
    quit                    exit

Every transfer prints a summary line with the elapsed time, the number of
retransmitted chunks and the effective throughput, which is what the
0% / 10% / 20% loss table in the report needs.

Usage:
    python3 UDPFTPClient.py <server_ip> <server_port> [--loss-rate 0.2]
"""

import argparse
import hashlib
import os
import random
import socket
import struct
import time

CHUNK_SIZE = 1000
HEADER_FORMAT = "!IBH"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)  # 7 bytes
ACK_FORMAT = "!I"
ACK_SIZE = struct.calcsize(ACK_FORMAT)        # 4 bytes
RECV_BUFFER = 2048
ACK_TIMEOUT = 0.5      # required default, see 2.4
MAX_RETRIES = 10       # consecutive timeouts before aborting a chunk
HANDSHAKE_RETRIES = 5  # control-message retries, see 2.1
IDLE_TIMEOUT = 15.0    # receiver gives up if the peer goes silent


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


def unique_path(path):
    """Returns ``path`` if free, else 'name (1).ext', 'name (2).ext', ..."""
    if not os.path.exists(path):
        return path
    stem, extension = os.path.splitext(path)
    counter = 1
    while os.path.exists("%s (%d)%s" % (stem, counter, extension)):
        counter += 1
    return "%s (%d)%s" % (stem, counter, extension)


def is_control(packet):
    """True if the datagram looks like an ASCII control message."""
    return packet.startswith((b"READY|", b"RESUME|", b"ERROR|", b"OK|",
                              b"DONE|", b"VERIFIED", b"MISMATCH"))


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


class Transport:
    """Wraps the UDP socket, the server address and the loss simulator."""

    def __init__(self, sock, server, loss_rate=0.0):
        self.sock = sock
        self.server = server
        self.loss_rate = loss_rate

    def send(self, payload, label=""):
        """Sends a datagram unless the loss simulator swallows it."""
        if self.loss_rate > 0 and random.random() < self.loss_rate:
            print("[loss] dropped outgoing %s" % label)
            return False
        self.sock.sendto(payload, self.server)
        return True

    def send_control(self, text):
        """Sends one ASCII control datagram."""
        return self.send(text.encode("ascii"), label=text)

    def receive(self, timeout):
        """Receives one datagram from the server, or None on timeout."""
        self.sock.settimeout(timeout)
        while True:
            try:
                packet, address = self.sock.recvfrom(RECV_BUFFER)
            except socket.timeout:
                return None
            if address == self.server:
                return packet
            # Ignore datagrams from anybody else (spoofed or stale).


def handshake(transport, message, expected_prefixes):
    """Sends a control message and waits for a reply, retrying 5 times."""
    for attempt in range(1, HANDSHAKE_RETRIES + 1):
        transport.send_control(message)
        reply = transport.receive(ACK_TIMEOUT)
        if reply is None:
            print("[rtx] no reply to %r, retry %d/%d"
                  % (message.split("|")[0], attempt, HANDSHAKE_RETRIES))
            continue
        if reply.startswith(b"ERROR|"):
            return reply
        if any(reply.startswith(prefix) for prefix in expected_prefixes):
            return reply
        # Anything else (a late ACK, a stray chunk) is ignored.
    return None


def do_put(transport, local_path):
    """Uploads a local file: client is the stop-and-wait sender."""
    if not os.path.isfile(local_path):
        print("[!] No such local file: %s" % local_path)
        return
    filename = os.path.basename(local_path)
    filesize = os.path.getsize(local_path)
    total_chunks = total_chunks_for(filesize)
    digest = md5_of_file(local_path)
    print("[*] PUT %s (%d bytes, %d chunks), local MD5 = %s"
          % (filename, filesize, total_chunks, digest))

    reply = handshake(transport,
                      "PUT|%s|%d|%d" % (filename, filesize, total_chunks),
                      (b"READY|",))
    if reply is None:
        print("[!] Server did not answer the PUT handshake.")
        return
    if reply.startswith(b"ERROR|"):
        print("[!] Server refused the upload: %s"
              % reply.decode("ascii", "replace"))
        return

    retransmissions = 0
    started = time.time()
    with open(local_path, "rb") as source:
        for seq_num in range(total_chunks):
            data = source.read(CHUNK_SIZE)
            is_last = (seq_num == total_chunks - 1)
            packet = make_data_packet(seq_num, is_last, data)

            retries = 0
            acked = False
            while not acked:
                transport.send(packet, label="DATA %d" % seq_num)
                deadline = time.time() + ACK_TIMEOUT
                while True:
                    remaining = deadline - time.time()
                    if remaining <= 0:
                        break
                    reply = transport.receive(remaining)
                    if reply is None:
                        break
                    if len(reply) == ACK_SIZE and not is_control(reply):
                        ack = struct.unpack(ACK_FORMAT, reply)[0]
                        if ack == seq_num:
                            acked = True
                            break
                        # Stale ACK: keep waiting for the one we need.
                if not acked:
                    retries += 1
                    retransmissions += 1
                    print("[rtx] timeout on chunk %d, retry %d/%d"
                          % (seq_num, retries, MAX_RETRIES))
                    if retries >= MAX_RETRIES:
                        print("Transfer failed: no ACK for chunk %d after "
                              "%d retries" % (seq_num, MAX_RETRIES))
                        return
            if (seq_num + 1) % 50 == 0 or is_last:
                print("[+] %d/%d chunks acknowledged"
                      % (seq_num + 1, total_chunks))

    elapsed = time.time() - started
    verdict = finish_as_sender(transport, digest)
    report(filename, "PUT", filesize, elapsed, retransmissions, verdict)


def finish_as_sender(transport, digest):
    """Sends DONE|<md5> and waits for the receiver's verdict."""
    for attempt in range(1, HANDSHAKE_RETRIES + 1):
        transport.send_control("DONE|%s" % digest)
        deadline = time.time() + ACK_TIMEOUT
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            reply = transport.receive(remaining)
            if reply is None:
                break
            if reply in (b"VERIFIED", b"MISMATCH"):
                return reply.decode("ascii")
        print("[rtx] no verdict for DONE, retry %d/%d"
              % (attempt, HANDSHAKE_RETRIES))
    return "NO VERDICT"


def do_get(transport, filename):
    """Downloads a remote file: client is the stop-and-wait receiver."""
    reply = handshake(transport, "GET|%s|0" % filename, (b"READY|",))
    if reply is None:
        print("[!] Server did not answer the GET handshake.")
        return
    if reply.startswith(b"ERROR|"):
        print("[!] %s" % reply.decode("ascii", "replace"))
        return

    fields = reply.decode("ascii").split("|")
    filesize = int(fields[1])
    total_chunks = int(fields[2])
    target = unique_path(os.path.basename(filename))
    if target != os.path.basename(filename):
        print("[*] %s already exists locally; saving as %s"
              % (filename, target))
    print("[*] GET %s (%d bytes, %d chunks) -> %s"
          % (filename, filesize, total_chunks, target))

    expected = 0
    duplicates = 0
    out_of_order = 0
    started = time.time()
    with open(target, "wb") as sink:
        while expected < total_chunks:
            packet = transport.receive(IDLE_TIMEOUT)
            if packet is None:
                print("[!] Server went silent after chunk %d; aborting."
                      % (expected - 1))
                return
            if is_control(packet):
                continue
            parsed = parse_data_packet(packet)
            if parsed is None:
                continue
            seq_num, is_last, data = parsed

            if seq_num == expected:
                sink.write(data)
                transport.send(struct.pack(ACK_FORMAT, seq_num),
                               label="ACK %d" % seq_num)
                expected += 1
                if expected % 50 == 0 or is_last:
                    print("[+] %d/%d chunks received"
                          % (expected, total_chunks))
                if is_last:
                    break
            elif seq_num < expected:
                duplicates += 1
                print("[dup] chunk %d already written, re-sending ACK %d"
                      % (seq_num, seq_num))
                transport.send(struct.pack(ACK_FORMAT, seq_num),
                               label="ACK %d" % seq_num)
            else:
                out_of_order += 1
                print("[ooo] chunk %d arrived early (expecting %d), "
                      "discarded without ACK" % (seq_num, expected))
    elapsed = time.time() - started

    verdict = finish_as_receiver(transport, target)
    print("[*] %d duplicate chunk(s), %d out-of-order chunk(s)"
          % (duplicates, out_of_order))
    report(filename, "GET", filesize, elapsed, duplicates, verdict)
    print("[+] Local MD5 = %s" % md5_of_file(target))


def finish_as_receiver(transport, path):
    """Waits for DONE|<md5>, answers VERIFIED or MISMATCH."""
    local_digest = md5_of_file(path)
    deadline = time.time() + 10.0
    verdict = "NO DONE RECEIVED"
    while time.time() < deadline:
        packet = transport.receive(0.5)
        if packet is None:
            continue
        if packet.startswith(b"DONE|"):
            digest = packet.decode("ascii", "replace").split("|", 1)[1].strip()
            verdict = "VERIFIED" if digest == local_digest else "MISMATCH"
            transport.send_control(verdict)
            # Linger briefly so a retransmitted DONE gets the same answer.
            linger = time.time() + 1.5
            while time.time() < linger:
                extra = transport.receive(0.5)
                if extra is not None and extra.startswith(b"DONE|"):
                    transport.send_control(verdict)
            return verdict
        parsed = parse_data_packet(packet)
        if parsed is not None:
            print("[dup] chunk %d after completion, re-sending ACK"
                  % parsed[0])
            transport.send(struct.pack(ACK_FORMAT, parsed[0]),
                           label="ACK %d" % parsed[0])
    return verdict


def do_list(transport):
    """Asks the server for its file listing (single datagram reply)."""
    reply = handshake(transport, "LIST", (b"OK|",))
    if reply is None:
        print("[!] Server did not answer LIST.")
        return
    fields = reply.decode("utf-8", "replace").split("|", 2)
    count = int(fields[1])
    print("[*] %d file(s) on the server:" % count)
    if count and len(fields) > 2:
        for entry in fields[2].split(";"):
            if not entry:
                continue
            name, _, size = entry.rpartition(",")
            print("    %-40s %s bytes" % (name, size))


def report(filename, operation, filesize, elapsed, retransmissions, verdict):
    """Prints the per-transfer line used to fill the loss-rate table."""
    throughput = filesize / 1024.0 / max(elapsed, 1e-9)
    print("[=] %s %s | %d bytes | %.3f s | %d retransmitted chunk(s) | "
          "%.2f KB/s | %s"
          % (operation, filename, filesize, elapsed, retransmissions,
             throughput, verdict))


def repl(transport):
    """Runs the interactive command prompt."""
    print("Commands: put <file> | get <file> | list | quit")
    while True:
        try:
            line = input("udp-ftp> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return
        if not line:
            continue
        parts = line.split(" ", 1)
        command = parts[0].lower()
        argument = parts[1].strip() if len(parts) > 1 else ""

        if command == "put" and argument:
            do_put(transport, argument)
        elif command == "get" and argument:
            do_get(transport, argument)
        elif command == "list":
            do_list(transport)
        elif command == "quit":
            return
        else:
            print("[!] Usage: put <file> | get <file> | list | quit")


def main():
    parser = argparse.ArgumentParser(
        description="Mini-FTP client (UDP, stop-and-wait).")
    parser.add_argument("host", help="server IP address, e.g. 127.0.0.1")
    parser.add_argument("port", type=int, help="server port")
    parser.add_argument("--loss-rate", type=float, default=0.0,
                        help="probability of dropping an outgoing datagram, "
                             "for local testing only (default 0.0)")
    args = parser.parse_args()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    transport = Transport(sock, (args.host, args.port), args.loss_rate)
    if args.loss_rate > 0:
        print("[*] Application-layer loss simulation active: %.0f%% "
              "(disable for graded runs)" % (args.loss_rate * 100))
    try:
        repl(transport)
    finally:
        sock.close()
        print("[*] Socket closed.")


if __name__ == "__main__":
    main()

# Mini-FTP — File Transfer over TCP and UDP

A simplified FTP-style file transfer tool implemented **twice**: once over **TCP**, where reliability is handled by the transport layer, and once over **UDP**, where a custom reliability layer (timeouts, acknowledgements, retransmission) is built from scratch on top of an unreliable transport.

The client connects to the server and supports three commands:

| Command      | Description                                  |
|--------------|-----------------------------------------------|
| `list`       | List all files currently stored on the server |
| `put <file>` | Upload a local file to the server             |
| `get <file>` | Download a file from the server               |

Both implementations share the same command set and user experience — the difference is entirely in how each one guarantees the data arrives intact.

## Features

- File upload, download, and listing over both TCP and UDP
- **TCP**: relies on the transport layer for in-order, lossless delivery; every transfer is additionally verified with a **SHA-256** checksum computed independently on both ends
- **UDP**: files are split into fixed-size (1000-byte) chunks, each chunk is numbered and individually acknowledged; unacknowledged chunks are retransmitted up to a retry limit; completed transfers are verified with an **MD5** hash
- Multi-threaded TCP server — multiple clients can upload/download concurrently without blocking one another
- Client never overwrites an existing local file on download — it auto-renames the incoming file instead
- Built-in artificial packet/ACK-loss simulator for testing the UDP retry logic under controlled loss rates
- Bonus: `ResumeUDPFTPClient.py` / `ResumeUDPFTPServer.py` — an extended UDP client/server pair that supports resuming an interrupted transfer instead of restarting it from scratch

## Project Structure

```
.
├── FTPServer.py              # TCP server
├── FTPClient.py               # TCP client
├── UDPFTPServer.py            # UDP server with custom reliability layer
├── UDPFTPClient.py            # UDP client with custom reliability layer
├── server_storage/            # Directory the server reads from / writes uploads to
└── README.md
```

## Requirements

- Python 3.6+
- Standard library only (`socket`, `threading`, `hashlib`, `os`, `sys`) — no third-party dependencies

## Part 1 — TCP File Transfer

Because TCP keeps a connection open and guarantees ordered, lossless delivery, the application code doesn't need to handle missing or out-of-order data itself — the transport layer takes care of that. To still be sure a file arrives byte-for-byte unchanged, every transfer is checked with a **SHA-256** checksum computed on both the sender's and receiver's copy.

### Running the server

```bash
python FTPServer.py <port>
```

### Running the client

```bash
python FTPClient.py <server_ip> <port>
```

Once connected, use `list`, `put <file>`, or `get <file>` at the `ftp>` prompt.

### Example session

```
ftp> list
Server contains [3] files:
 - ACN.pdf 1394433
 - ACNTut.pdf 423569
 - HTTP_GET.pdf 119428

ftp> put ACNTut.pdf
Calculating local file checksum...
SHA-256: 9c30ce997ef02a565e591c2d67569b34002b186b5e239bf91e94b55463afb48f
Uploading ACNTut.pdf (423569 bytes)...
Success: Transferred 423569 total bytes cleanly.

ftp> get ACN.pdf
Note: Local file already exists. Auto-renamed destination target to: ACN_1.pdf
Success: Downloaded 1394433 bytes to ACN_1.pdf.
Verifying integrity checksum of downloaded file...
SHA-256: e95af85bb0af1392adbf6a3ecb3b87ed6ccd0195a1e830d6b09140bafa8e9ce0
```

### Concurrency

The server spawns a new thread per connecting client, so multiple clients can `put`/`get` at the same time without blocking one another. This was verified by running two clients concurrently against the same server, each uploading and downloading different files in parallel.

## Part 2 — UDP File Transfer with Reliability

UDP guarantees neither delivery nor ordering, so this part implements a small reliability layer on top of it:

1. The file is split into **fixed-size, ~1000-byte chunks**, each tagged with a sequence number.
2. The receiver sends an acknowledgement (ACK) for every chunk it receives.
3. If the sender doesn't see an ACK within the timeout window, it resends that chunk — up to **10 retries** per data chunk (control/`DONE` messages are retried up to 5 times).
4. Once the full file is reassembled, both sides compute an **MD5** hash and the receiver reports whether it matches — `VERIFIED` or `NO VERDICT` if the final acknowledgement itself couldn't get through.

### Running the server

```bash
python UDPFTPServer.py <port>
```

### Running the client

```bash
python UDPFTPClient.py <server_ip> <port>
```

Commands at the `udp-ftp>` prompt: `put <file>`, `get <file>`, `list`, `quit`.

### Example session (no loss)

```
udp-ftp> put ACNTut.pdf
[*] PUT ACNTut.pdf (423569 bytes, 424 chunks), local MD5 = 9d745fef1cb57daa837fbdeaef15dddd
[+] 50/424 chunks acknowledged
...
[+] 424/424 chunks acknowledged
[=] PUT ACNTut.pdf | 423569 bytes | 1.541 s | 0 retransmitted chunk(s) | 268.51 KB/s | VERIFIED
```

### Simulating packet loss

The client includes a built-in artificial loss simulator used to stress-test the retry logic (see `run_udp_test.sh` for example invocations at 10% and 20% simulated loss). At these rates, some chunks — or their ACKs — are deliberately dropped, forcing the sender to time out and resend:

```
[rtx] timeout on chunk 1153, retry 1/10
[rtx] timeout on chunk 1161, retry 1/10
...
[=] PUT short.pdf | 1394433 bytes | 80.224 s | 140 retransmitted chunk(s) | 16.97 KB/s | VERIFIED
```

At higher loss (20%), enough final acknowledgements can be dropped that the sender exhausts its retries on the closing `DONE` handshake, in which case the transfer still completes correctly but is reported as `NO VERDICT` rather than `VERIFIED` (the data itself still arrived and matched — only the final ACK was lost).

## Performance: Loss vs. Throughput

| Operation | File          | Size (bytes) | Simulated Loss | Time (s) | Retransmitted Chunks | Throughput (KB/s) |
|-----------|---------------|--------------|-----------------|----------|------------------------|--------------------|
| PUT       | short.pdf     | 1,394,433    | 0%              | 3.88     | 0                      | 350.86             |
| PUT       | short.pdf     | 1,394,433    | 10%             | 80.22    | 140                    | 16.97              |
| PUT       | short.pdf     | 1,394,433    | 20%             | 169.10   | 321                    | 8.05               |
| GET       | ACN.pdf       | 1,394,433    | 0%              | 6.53     | 0                      | 208.54             |
| GET       | ACN.pdf       | 1,394,433    | 10%             | 90.35    | 165                    | 15.07              |
| GET       | ACN.pdf       | 1,394,433    | 20%             | 171.30   | 323                    | 7.95               |

**Takeaway:** loss and performance move in lockstep. Going from 0% to 10% simulated loss already pushes retransmissions from zero into the hundreds and multiplies transfer time by more than 20×. Doubling the loss again to 20% roughly doubles both the retransmission count and the time taken, while throughput keeps falling — from the hundreds of KB/s at 0% loss down to single digits at 20%. Every dropped packet costs a full timeout-and-resend cycle, and those cycles compound quickly once loss rises past a few percent.

## Design Notes

- **No accidental overwrites**: if a `get` target already exists locally, the client doesn't touch it — it auto-generates a new filename (e.g. `ACN_1.pdf`, `ACN_2.pdf`) and saves the download there instead.
- **Integrity checking**: the TCP path uses SHA-256, computed before upload and re-verified after download; the UDP path uses MD5, computed by both sender and receiver once the transfer completes and exchanged as part of the closing handshake.
- **Chunking**: UDP files are broken into 1000-byte chunks; a 1.3 MB file works out to roughly 1,395 chunks, and reassembly was verified to work correctly at that scale as well as for small files.


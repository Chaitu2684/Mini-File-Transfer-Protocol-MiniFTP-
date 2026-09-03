import sys
import os
import socket
import hashlib


def read_line(conn):
    """Reads byte-by-byte from socket stream until reaching a clean '\\n'."""
    line_bytes = bytearray()
    while True:
        chunk = conn.recv(1)
        if not chunk:
            return None
        line_bytes.extend(chunk)
        if chunk == b'\n':
            return line_bytes.decode('ascii').strip('\n')


def recv_exact(conn, num_bytes):
    """Collects stream data until the buffer safely matches explicit byte limits."""
    buffer = bytearray()
    while len(buffer) < num_bytes:
        to_read = num_bytes - len(buffer)
        chunk = conn.recv(min(to_read, 4096))
        if not chunk:
            return None
        buffer.extend(chunk)
    return bytes(buffer)


def get_unique_filename(filename):
    """Prevents file overwrite hazards by adding numerical incremental suffixes."""
    if not os.path.exists(filename):
        return filename
    name, ext = os.path.splitext(filename)
    counter = 1
    while os.path.exists(f"{name}_{counter}{ext}"):
        counter += 1
    return f"{name}_{counter}{ext}"


def calculate_sha256(filepath):
    """Computes the SHA-256 checksum"""
    sha256_hash = hashlib.sha256()
    try:
        with open(filepath, "rb") as f:
            # Read in chunks to efficiently handle large files (>1MB) without high memory use
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except Exception as e:
        return f"Error calculating hash: {e}"


def main():
    if len(sys.argv) != 3:
        print("Usage: python3 TCPClient.py <server_ip> <server_port>")
        sys.exit(1)

    ip = sys.argv[1]
    try:
        port = int(sys.argv[2])
    except ValueError:
        print("Error: Port must be an integer.")
        sys.exit(1)

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client.connect((ip, port))
        print(f"✅ Connected to the FTP server at {ip}:{port}")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        sys.exit(1)

    try:
        while True:
            user_input = input("ftp> ").strip()
            if not user_input:
                continue

            parts = user_input.split(" ", 1)
            cmd = parts[0].lower()

            if cmd == "put":
                if len(parts) < 2:
                    print("Usage: put <local_filename>")
                    continue
                local_file = parts[1]

                if not os.path.exists(local_file) or not os.path.isfile(local_file):
                    print("❌ Error: Local file does not exist.")
                    continue

                filesize = os.path.getsize(local_file)
                filename = os.path.basename(local_file)

                # Calculate hash before sending
                print("🔒 Calculating local file checksum...")
                local_hash = calculate_sha256(local_file)
                print(f"   SHA-256: {local_hash}")

                print(f"⏳ Uploading {filename} ({filesize} bytes)...")
                try:
                    with open(local_file, "rb") as f:
                        file_data = f.read()

                    header = f"PUT {filename} {filesize}\n".encode('ascii')
                    client.sendall(header + file_data)

                    response = read_line(client)
                    if response and response.startswith("OK"):
                        print(f"✅ Success: Transferred {filesize} total bytes cleanly.")
                    else:
                        print(f"❌ Server Error: {response if response else 'No response'}")
                except Exception as e:
                    print(f"❌ Upload action aborted: {e}")

            elif cmd == "get":
                if len(parts) < 2:
                    print("Usage: get <remote_filename>")
                    continue
                remote_file = parts[1]

                header = f"GET {remote_file}\n".encode('ascii')
                client.sendall(header)

                response = read_line(client)
                if not response:
                    print("❌ Error: Lost server communication connection mid-transaction.")
                    break

                if response.startswith("OK"):
                    resp_parts = response.split(" ")
                    try:
                        filesize = int(resp_parts[1])
                    except (IndexError, ValueError):
                        print("❌ Error: Received malformed length indicator frame from server.")
                        continue

                    file_data = recv_exact(client, filesize)
                    if file_data is None or len(file_data) != filesize:
                        print("❌ Error: File payload transfer interrupted.")
                        continue

                    target_name = get_unique_filename(remote_file)
                    if target_name != remote_file:
                        print(f"⚠️ Note: Local file already exists. Auto-renamed destination target to: {target_name}")

                    try:
                        with open(target_name, "wb") as f:
                            f.write(file_data)
                        print(f"✅ Success: Downloaded {filesize} bytes to {target_name}.")

                        # Verify integrity right after writing to disk
                        print("🔒 Verifying integrity checksum of downloaded file...")
                        download_hash = calculate_sha256(target_name)
                        print(f"   SHA-256: {download_hash}")

                    except Exception as e:
                        print(f"❌ File system access failure writing to disk: {e}")
                else:
                    print(f"❌ Server Error: {response}")

            elif cmd == "list":
                client.sendall(b"LIST\n")
                response = read_line(client)
                if not response:
                    print("❌ Error: Did not receive data back from server structure.")
                    break

                if response.startswith("OK"):
                    resp_parts = response.split(" ")
                    try:
                        count = int(resp_parts[1])
                    except (IndexError, ValueError):
                        print("❌ Error: Received malformed list count metadata context.")
                        continue

                    print(f"📂 Server contains [{count}] files:")
                    for _ in range(count):
                        line = read_line(client)
                        if line:
                            print(f" - {line}")
                else:
                    print(f"❌ Server Error: {response}")

            elif cmd == "quit":
                client.sendall(b"QUIT\n")
                print("Closing client connection.")
                break
            else:
                print("Unknown command. Use: put, get, list, or quit")

    except Exception as e:
        print(f"\nAn exceptional connectivity state forced shutdown: {e}")
    finally:
        client.close()


if __name__ == "__main__":
    main()

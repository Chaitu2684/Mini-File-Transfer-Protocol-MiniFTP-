import socket
import threading
import os
import sys

STORAGE_DIR = "server_storage"

def read_line(sock):
    """Reads a single line byte-by-byte until '\n' is found."""
    line = b""
    while True:
        char = sock.recv(1)
        if not char:
            break
        if char == b'\n':
            break
        line += char
    return line.decode('ascii', errors='ignore')

def recv_exact(sock, count):
    """Receives exactly 'count' bytes from the socket."""
    buf = bytearray()
    while len(buf) < count:
        packet = sock.recv(count - len(buf))
        if not packet:
            return None
        buf.extend(packet)
    return bytes(buf)

def handle_client(conn, addr):
    client_ip = f"{addr[0]}:{addr[1]}"
    print(f"[+] {client_ip} connected")
    
    try:
        while True:
            cmd_line = read_line(conn)
            if not cmd_line:
                break
            
            parts = cmd_line.strip().split()
            if not parts:
                continue
            
            cmd = parts[0].upper()
            
            if cmd == "PUT" and len(parts) == 3:
                filename = parts[1]
                filesize = int(parts[2])
                print(f"[+] {client_ip} uploading {filename} ({filesize} bytes)")
                
                file_data = recv_exact(conn, filesize)
                if file_data is None or len(file_data) < filesize:
                    print(f"[-] {client_ip} disconnected mid-transfer")
                    break
                
                filepath = os.path.join(STORAGE_DIR, filename)
                try:
                    with open(filepath, 'wb') as f:
                        f.write(file_data)
                    conn.sendall(f"OK {filesize}\n".encode('ascii'))
                    print(f"[+] Saved {filename}")
                except Exception as e:
                    conn.sendall(f"ERR {str(e)}\n".encode('ascii'))
                    print(f"[-] Failed to save {filename}: {e}")
                    
            elif cmd == "GET" and len(parts) == 2:
                filename = parts[1]
                filepath = os.path.join(STORAGE_DIR, filename)
                if not os.path.exists(filepath):
                    conn.sendall(b"ERR file not found\n")
                    print(f"[-] {client_ip} requested non-existent file {filename}")
                else:
                    filesize = os.path.getsize(filepath)
                    print(f"[+] {client_ip} downloading {filename} ({filesize} bytes)")
                    conn.sendall(f"OK {filesize}\n".encode('ascii'))
                    with open(filepath, 'rb') as f:
                        conn.sendall(f.read())
                        
            elif cmd == "LIST":
                files = os.listdir(STORAGE_DIR)
                conn.sendall(f"OK {len(files)}\n".encode('ascii'))
                for f in files:
                    size = os.path.getsize(os.path.join(STORAGE_DIR, f))
                    conn.sendall(f"{f} {size}\n".encode('ascii'))
                print(f"[+] {client_ip} listed files")
                
            elif cmd == "QUIT":
                break
    except Exception as e:
        print(f"[-] Error with {client_ip}: {e}")
    finally:
        conn.close()
        print(f"[-] {client_ip} disconnected")

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 FTPServer.py <port>")
        sys.exit(1)
        
    port = int(sys.argv[1])
    os.makedirs(STORAGE_DIR, exist_ok=True)

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('0.0.0.0', port))
    server.listen(5)
    
    print(f"[*] Server listening on port {port}")
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()

if __name__ == "__main__":
    main()

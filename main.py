import asyncio
import os
import socket
import sys

# ============ CONFIGURATION ============
PROXY_PORT = int(os.environ.get('PORT', 8080))
# Speed နှင့် Throughput အမြင့်ဆုံးရရှိရန် Buffer 128KB သို့ တိုးမြှင့်ထားသည်
BUFFER_SIZE = 128 * 1024  


def optimize_socket(sock: socket.socket):
    """ High-Performance Socket Optimization Settings """
    try:
        # Latency (Ping) ကို လျှော့ချရန် TCP_NODELAY သုံးသည်
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        # Connection မပြတ်စေရန် Keep-Alive သတ်မှတ်သည်
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        # Low-level Kernel Network Buffers များကို မြှင့်တင်သည်
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, BUFFER_SIZE)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, BUFFER_SIZE)
    except Exception:
        pass


async def pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """ High-Throughput Ultra-Fast Pipe """
    try:
        while True:
            data = await reader.read(BUFFER_SIZE)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except (asyncio.CancelledError, ConnectionResetError, Exception):
        pass
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    # Socket Optimization ကို Client connection တွင် ချက်ချင်း ထည့်သွင်းသည်
    try:
        sock = writer.get_extra_info('socket')
        if sock:
            optimize_socket(sock)
    except Exception:
        pass

    try:
        # --- SOCKS5 Handshake Phase ---
        version = await asyncio.wait_for(reader.readexactly(1), timeout=15.0)
        if version != b'\x05':
            writer.close()
            return

        nmethods_byte = await reader.readexactly(1)
        await reader.readexactly(nmethods_byte[0])

        # Force Accept NO AUTH (0x00)
        writer.write(b'\x05\x00')
        await writer.drain()

        # --- Request Phase ---
        req_header = await asyncio.wait_for(reader.readexactly(4), timeout=15.0)
        cmd, atyp = req_header[1], req_header[3]

        if cmd != 1:  # CONNECT Command သာ လက်ခံသည်
            writer.write(b'\x05\x07\x00\x01\x00\x00\x00\x00\x00\x00')
            await writer.drain()
            writer.close()
            return

        if atyp == 1:  # IPv4
            addr_bytes = await reader.readexactly(4)
            target_host = socket.inet_ntoa(addr_bytes)
        elif atyp == 3:  # Domain Name
            dlen_byte = await reader.readexactly(1)
            domain = await reader.readexactly(dlen_byte[0])
            target_host = domain.decode('utf-8', errors='ignore')
        elif atyp == 4:  # IPv6
            addr_bytes = await reader.readexactly(16)
            target_host = socket.inet_ntop(socket.AF_INET6, addr_bytes)
        else:
            writer.write(b'\x05\x08\x00\x01\x00\x00\x00\x00\x00\x00')
            await writer.drain()
            writer.close()
            return

        port_bytes = await reader.readexactly(2)
        target_port = int.from_bytes(port_bytes, 'big')

        # --- Target Connection & Forwarding ---
        try:
            target_reader, target_writer = await asyncio.wait_for(
                asyncio.open_connection(target_host, target_port),
                timeout=15.0
            )

            # Target Socket ကိုလည်း Low-latency အတွက် Optimize လုပ်သည်
            target_sock = target_writer.get_extra_info('socket')
            if target_sock:
                optimize_socket(target_sock)

            # Connect Success Response
            writer.write(b'\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00')
            await writer.drain()

            # Bidirectional Fast Streaming
            await asyncio.gather(
                pipe(reader, target_writer),
                pipe(target_reader, writer),
                return_exceptions=True
            )

        except Exception:
            writer.write(b'\x05\x05\x00\x01\x00\x00\x00\x00\x00\x00')
            await writer.drain()
            writer.close()

    except Exception:
        try:
            writer.close()
        except Exception:
            pass


async def main():
    server = await asyncio.start_server(
        handle_client, '0.0.0.0', PROXY_PORT,
        limit=128 * 1024,
        backlog=2048  # High Concurrent Connections များပြားပါက Queue လက်ခံနိုင်ရန်
    )

    print("=" * 50, flush=True)
    print("⚡ EXTREME HIGH-SPEED SOCKS5 PROXY", flush=True)
    print(f"📌 Internal Port: {PROXY_PORT}", flush=True)
    print(f"🚀 Buffer Size: 128 KB", flush=True)
    print(f"⚡ Socket TCP_NODELAY & Keepalive: ENABLED", flush=True)
    print("=" * 50, flush=True)

    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopping proxy...")

import asyncio
import os
import socket
import sys

# ============ CONFIGURATION ============
# JustRunMy.App က PORT environment variable ကို auto-set လုပ်ပေးပါတယ်
PROXY_PORT = int(os.environ.get('PORT', 8080)) 

print("=" * 50, flush=True)
print(" SOCKS5 Proxy Server Starting...", flush=True)
print(f"📌 Internal Port: {PROXY_PORT}", flush=True)
print("🔓 Mode: NO AUTHENTICATION (Open Access)", flush=True)
print("=" * 50, flush=True)


async def handle_client(reader, writer):
    try:
        # --- SOCKS5 Handshake Phase ---
        version = await asyncio.wait_for(reader.readexactly(1), timeout=15.0)
        if version != b'\x05':
            writer.close()
            return

        nmethods_byte = await reader.readexactly(1)
        nmethods = nmethods_byte[0]
        methods = await reader.readexactly(nmethods)

        # 🔥 IMPORTANT CHANGE HERE 🔥
        # We force accept method 0x00 (No Auth) regardless of what client sends
        writer.write(b'\x05\x00') 
        await writer.drain()

        # --- Connection Request Phase ---
        req_ver = await reader.readexactly(1)
        cmd = await reader.readexactly(1)
        rsv = await reader.readexactly(1)
        atyp = await reader.readexactly(1)

        if cmd != b'\x01':
            writer.write(b'\x05\x07\x00\x01\x00\x00\x00\x00\x00\x00')
            await writer.drain()
            writer.close()
            return

        target_host = None
        target_port = 0
        
        if atyp == b'\x01':  # IPv4
            addr_bytes = await reader.readexactly(4)
            port_bytes = await reader.readexactly(2)
            target_host = socket.inet_ntoa(addr_bytes)
        elif atyp == b'\x03':  # Domain Name
            dlen_byte = await reader.readexactly(1)
            dlen = dlen_byte[0]
            domain = await reader.readexactly(dlen)
            port_bytes = await reader.readexactly(2)
            target_host = domain.decode('utf-8', errors='replace')
        elif atyp == b'\x04':  # IPv6
            addr_bytes = await reader.readexactly(16)
            port_bytes = await reader.readexactly(2)
            parts = []
            for i in range(0, 16, 2):
                parts.append(f'{addr_bytes[i]:02x}{addr_bytes[i+1]:02x}')
            target_host = ':'.join(parts)
        else:
            writer.write(b'\x05\x08\x00\x01\x00\x00\x00\x00\x00\x00')
            await writer.drain()
            writer.close()
            return

        target_port = int.from_bytes(port_bytes, 'big')

        # --- Target Connection & Forwarding ---
        try:
            target_reader, target_writer = await asyncio.wait_for(
                asyncio.open_connection(target_host, target_port),
                timeout=15.0
            )

            writer.write(b'\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00')
            await writer.drain()

            print(f"✅ Connected: {target_host}:{target_port}", flush=True)

            async def forward(src, dst):
                try:
                    while True:
                        # Buffer size ကို 4KB လျှော့ချထားပါတယ် (Stability အတွက်)
                        data = await src.read(4096) 
                        if not data:
                            break
                        dst.write(data)
                        await dst.drain()
                except Exception:
                    pass
                finally:
                    try:
                        dst.close()
                    except:
                        pass

            await asyncio.gather(
                forward(reader, target_writer),
                forward(target_reader, writer)
            )

        except Exception as e:
            print(f"❌ Connection failed: {target_host}:{target_port} -> {e}", flush=True)
            writer.write(b'\x05\x05\x00\x01\x00\x00\x00\x00\x00\x00')
            await writer.drain()
            writer.close()

    except Exception as e:
        try:
            writer.close()
        except:
            pass


async def main():
    server = await asyncio.start_server(
        handle_client, '0.0.0.0', PROXY_PORT
    )
    
    print(f"🚀 SOCKS5 Proxy is running!", flush=True)
    print(f"   Bind: 0.0.0.0:{PROXY_PORT}", flush=True)
    print(f"   Status: OPEN ACCESS", flush=True)

    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopping proxy...")

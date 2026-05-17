import sys
import socket
import os

print("--- DIAGNOSTIC TOOL ---")

# 1. Check Python Modules
print("\n[1] Checking Python Modules...")
modules = ['flask', 'redis', 'requests', 'bs4', 'pandas']
for mod in modules:
    try:
        __import__(mod)
        print(f"✅ {mod}: Installed")
    except ImportError:
        print(f"❌ {mod}: MISSING (Run: pip install {mod})")

# 2. Check Redis Connection
print("\n[2] Checking Redis Connection (localhost:6379)...")
try:
    import redis
    r = redis.Redis(host='localhost', port=6379, socket_connect_timeout=2)
    if r.ping():
        print("✅ Redis is RUNNING and accessible.")
        print(f"   - Keys in DB: {len(r.keys('*'))}")
except Exception as e:
    print(f"❌ Redis Connection FAILED: {e}")
    print("   -> Make sure Redis server is started!")

# 3. Check Port 5000 (Dashboard)
print("\n[3] Checking Port 5000 (Dashboard)...")
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
result = sock.connect_ex(('127.0.0.1', 5000))
if result == 0:
    print("⚠️ Port 5000 is OPEN (Dashboard might be running already).")
else:
    print("ℹ️ Port 5000 is CLOSED (Dashboard is not running yet).")
sock.close()

print("\n--- END DIAGNOSTICS ---")

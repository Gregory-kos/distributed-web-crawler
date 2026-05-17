import subprocess
import time
import os
import signal
import sys
import webbrowser

procs = []

def stop(s,f):
    print("\n Shutting down...")
    for p in procs: p.terminate()
    sys.exit(0)

signal.signal(signal.SIGINT, stop)

def main():
    os.system('cls' if os.name == 'nt' else 'clear')
    
    # 1. Change to Script Directory (CRITICAL FIX)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    
    print(f"--- LAUNCHER ---")
    print(f"Root: {script_dir}")
    
    # 2. Start Master
    print("Starting Master...")
    if os.name == 'nt':
        # Simple command relying on CWD
        p1 = subprocess.Popen('start cmd /k "python master_node.py"', shell=True)
    else:
        p1 = subprocess.Popen(["python3", "master_node.py"])
    procs.append(p1)
    
    time.sleep(4) 

    # 3. Start Workers
    for i in range(3):
        print(f"Starting Worker {i+1}...")
        if os.name == 'nt':
            p = subprocess.Popen(f'start cmd /k "python worker_node.py"', shell=True)
        else:
            p = subprocess.Popen(["python3", "worker_node.py"])
        procs.append(p)
        time.sleep(1.5)

    print("\n SYSTEM LIVE")
    webbrowser.open("http://localhost:5000")
    
    while True: time.sleep(1)

if __name__ == "__main__":
    main()

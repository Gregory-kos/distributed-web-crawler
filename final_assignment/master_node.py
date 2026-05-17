import redis
import threading
import time
import sys
import json
import random
import os
import subprocess
from config import *
from utils import Colors
from database import DatabaseManager
from dijkstra import DijkstraScholtenManager
from custom_queue import DistributedQueue

'''
Ο Master Node είναι ο κεντρικός συντονιστής του συστήματος crawler. Είναι υπεύθυνος για την αρχικοποίηση του συστήματος, 
τη διαχείριση των εργασιών, την παρακολούθηση της υγείας των workers, και τον τερματισμό του συστήματος όταν ολοκληρωθεί η εργασία.
Ο Master Node λειτουργεί σε ένα άπειρο loop, όπου εκτελεί διάφορες εργασίες όπως το heartbeat για να δηλώσει ότι είναι ζωντανός, 
και την παρακολούθηση της κατάστασης του συστήματος για να ανιχνεύσει πότε έχει ολοκληρωθεί η εργασία.

'''


class MasterNode:
    def __init__(self, seeds):
        self.seeds = seeds
        # 1. CONNECT TO REDIS
        try:
            # self.r ειναι η σύνδεση με το Redis, χρησιμοποιούμε τα config constants για host, port, db, και decode_responses=True για να δουλεύουμε με strings αντί για bytes. Κάνουμε ένα ping για να βεβαιωθούμε ότι η σύνδεση είναι επιτυχής.
            # Αν το ping αποτύχει, θα πιαστεί η εξαίρεση
            # για να βαιθούουμε ότι το Redis είναι απαραίτητο για τη λειτουργία του συστήματος, αν δεν μπορούμε να συνδεθούμε θα τερματίσουμε το πρόγραμμα με sys.exit(1).
            self.r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
            self.r.ping()
            print(f"{Colors.GREEN} Connected to Redis at {REDIS_HOST}{Colors.ENDC}")
        except Exception as e:
            print(f"{Colors.FAIL} Redis Error: {e}{Colors.ENDC}")
            sys.exit(1)
        #2. INITIALIZE COMPONENTS
        # self.db είναι ο διαχειριστής της βάσης δεδομένων, 
        # self.ds είναι ο διαχειριστής του αλγορίθμου Dijkstra-Scholten για τον τερματισμό, 
        # και self.queue είναι η υλοποίηση της κατανεμημένης ουράς που χρησιμοποιούμε για να διαχειριστούμε τα URLs
        # που πρέπει να επεξεργαστούν οι workers.
        self.db = DatabaseManager(reset=True) 
        self.ds = DijkstraScholtenManager(self.r)
        self.queue = DistributedQueue(self.r, KEY_QUEUE)

    def heartbeat(self):
        """Updates Master status every 2 seconds"""
        #Η μέθοδος heartbeat τρέχει σε ένα άπειρο loop και κάθε 2 δευτερόλεπτα ανανεώνει το "crawler:leader" key στο Redis με την τιμή "MASTER"
        # και expiration 10 δευτερολέπτων. Αυτό λειτουργεί ως ένας τρόπος να δηλώσουμε ότι ο Master είναι ζωντανός και ενεργός.
        # Αν για κάποιο λόγο ο Master σταματήσει να τρέχει, το key θα λήξει μετά από 10 δευτερόλεπτα, και οι workers μπορούν να ανιχνεύσουν αυτή την κατάσταση και να αντιδράσουν ανάλογα (π.χ. εκλέγοντας νέο leader).
        while True:
            try:
                # Renew Leader Lock
                self.r.set("crawler:leader", "MASTER", ex=10)
                
                # Update Status
                state = {
                    'id': "MASTER", 
                    'status': 'LEADER', 
                    'jobs': self.r.llen(KEY_RESULTS), 
                    'cpu': 1, 
                    'memory': 50, 
                    'avg_latency': 0, 
                    'last_seen': time.time(),
                    'current_url': 'Coordinating Fleet...',
                    'is_master_node': True
                }
                self.r.setex("crawler:worker_status:MASTER", 10, json.dumps(state))
            except Exception as e:
                print(f"Heartbeat Error: {e}")
            time.sleep(2)

    def monitor_termination(self):
        """Checks for completion"""
        #monitor_termination τρέχει σε ξεχωριστό thread και κάθε 2 δευτερόλεπτα ελέγχει την κατάσταση του συστήματος μέσω της μεθόδου self.ds.get_status().
# Αν το status δείχνει ότι το σύστημα έχει τερματιστεί (terminated=True) και δεν υπάρχει κανένα έλλειμμα (total_deficit=0), τότε ο Master θεωρεί ότι η εργασία έχει ολοκληρωθεί επιτυχώς.
# # Σε αυτή την περίπτωση, ο Master τυπώνει ένα μήνυμα "SYSTEM TERMINATED!", καταγράφει αυτό το γεγονός στα logs του Redis, και θέτει ένα key "crawler:shutdown" με τιμή "1" για να ενημερώσει τους workers ότι πρέπει να τερματίσουν.
        print(f"{Colors.BLUE} Monitor Thread Started{Colors.ENDC}")
        time.sleep(5)
        while True:
            try:
                
                status = self.ds.get_status()
                if status['terminated'] and status['total_deficit'] == 0:
                    print(f"\n{Colors.GREEN} SYSTEM TERMINATED!{Colors.ENDC}")
                    self.r.lpush("crawler:logs", " SYSTEM TERMINATED.")
                    self.r.set("crawler:shutdown", "1")
                    break
            except: pass
            time.sleep(2)
    # Η μέθοδος start_dashboard είναι υπεύθυνη για την εκκίνηση του dashboard ως ξεχωριστή διεργασία.
    # Χρησιμοποιεί το subprocess.Popen για να εκτελέσει το dashboard_app.py σε ένα νέο παράθυρο κονσόλας .
    # Αν η εκκίνηση του dashboard αποτύχει, θα πιαστεί η εξαίρεση και θα τυπωθεί ένα μήνυμα σφάλματος.
    def start_dashboard(self):
        """Launches the dashboard as a separate process"""
        try:
            cwd = os.path.dirname(os.path.abspath(__file__))
            print(f"{Colors.GREEN} Launching Dashboard Process...{Colors.ENDC}")
            
            # Launch dashboard_app.py as a separate process
            if os.name == 'nt':
                 subprocess.Popen([sys.executable, "dashboard_app.py"], cwd=cwd, creationflags=subprocess.CREATE_NEW_CONSOLE)
            else:
                 subprocess.Popen([sys.executable, "dashboard_app.py"], cwd=cwd)
                 
            print(f"{Colors.GREEN} Dashboard running at http://localhost:5000{Colors.ENDC}")
        except Exception as e:
            print(f" Failed to launch dashboard: {e}")

    # Η μέθοδος monitor_fleet είναι υπεύθυνη για την ενεργή διαχείριση του στόλου των workers.
    # Τρέχει σε ένα άπειρο loop και κάθε 5 δευτερόλεπτα ελέγχει την κατάσταση όλων των workers που έχουν καταγραφεί στο Redis.
    def monitor_fleet(self):
        """Active Fleet Management: Detects and cleans up dead workers"""
        print(f"{Colors.BLUE} Fleet Monitor Active{Colors.ENDC}")
        while True:
            try:
                # Get all worker keys
                #Ανακτά όλα τα keys που ταιριάζουν με το pattern "crawler:worker_status:*" για να πάρει την κατάσταση όλων των workers.
                keys = self.r.keys("crawler:worker_status:*")
                now = time.time()
                
                for k in keys:
                    if "MASTER" in k: continue # Don't kill self , hh 
                    
                    try:
                        #
                        data = json.loads(self.r.get(k))
                        last_seen = data.get('last_seen', 0)
                        
                        # If silent for > 15s -> DEAD
                        if now - last_seen > 15:
                            wid = data.get('id', 'Unknown')
                            
                            print(f"{Colors.FAIL} Worker {wid} is DEAD (Timeout). Cleaning up...{Colors.ENDC}")
                            
                            # --- RELIABLE RECOVERY (Check Processing Queue) ---
                            # Check if the worker died while holding an item
                            processing_key = f"crawler:processing:{wid}"
                            pending_items = self.r.lrange(processing_key, 0, -1)
                            
                            for item_str in pending_items:
                                try:
                                    item = json.loads(item_str)
                                    url = item.get('url') if isinstance(item, dict) else item
                                    depth = item.get('depth', 0) if isinstance(item, dict) else 0
                                    parent = item.get('parent', 'Master')
                                    
                                    ts = time.strftime("[%H:%M:%S]")
                                    log_msg = f"{ts} MASTER: ⚠ Worker {wid} DIED! Recovering: {url[:30]}..."
                                    self.r.lpush("crawler:logs", log_msg)
                                    self.r.ltrim("crawler:logs", 0, 49)
                                    
                                    # Re-Queue with original parent
                                    self.queue.push({'url': url, 'depth': depth, 'parent': parent})
                                    
                                    print(f"{Colors.WARNING} Recovered job: {url}{Colors.ENDC}")
                                except Exception as re:
                                    print(f"Recovery Error: {re}")
                            
                            # Clean up processing list
                            self.r.delete(processing_key)

                            # --- DS DEFICIT TRANSFER (ATOMIC) ---
                            self.ds.transfer_deficit(wid, "Master")

                            # 1. Remove Status
                            self.r.delete(k)
                            
                            # 2. Release Locks (if any held by this worker)
                            self.r.srem("rw:active_readers", wid)
                            
                            writer = self.r.get("rw:writer")
                            if writer == wid:
                                self.r.delete("rw:writer")
                                print(f"{Colors.WARNING} Released Write Lock held by dead worker {wid}{Colors.ENDC}")
                                
                    except: pass
            except Exception as e:
                print(f"Monitor Error: {e}")
            
            time.sleep(5)
    #Η μέθοδος start είναι υπεύθυνη για την εκκίνηση του Master Node και την αρχικοποίηση όλων των απαραίτητων διαδικασιών.
    def start(self):
        print(f"{Colors.BLUE} Master Starting Initialization...{Colors.ENDC}")
        
        # 2. CLEANUP
        self.r.flushall()
        print(" Redis Memory Wiped.")
        self.ds.reset()
        
        # 3. SEEDING
        print(f" Seeding {len(self.seeds)} URLs...")
        for url in self.seeds:
            self.queue.push({'url': url, 'depth': 0, 'parent': 'Master'})
            self.r.sadd(KEY_VISITED, url)
            print(f"   -> Pushed: {url}")
            
        self.r.set("crawler:config:depth", 3)
        self.r.hincrby(KEY_DS_DEFICIT, "Master", len(self.seeds))
        
        # 4. VERIFY QUEUE
        q_len = self.r.llen(KEY_QUEUE)
        print(f"{Colors.GREEN} Queue Verification: {q_len} items in Redis.{Colors.ENDC}")

        # 5. START THREADS
        t_mon = threading.Thread(target=self.monitor_termination, daemon=True)
        t_mon.start()
        
        # NEW: Fleet Monitor
        t_fleet = threading.Thread(target=self.monitor_fleet, daemon=True)
        t_fleet.start()

        # 7. MAIN LOOP (Heartbeat) - Runs in Main Thread
        print(f"{Colors.BLUE} Master is Running (Press Ctrl+C to stop)...{Colors.ENDC}")
        self.heartbeat()

if __name__ == "__main__":
    seeds = [
        "https://www.python.org/", "https://en.wikipedia.org/wiki/Main_Page",
        "https://news.ycombinator.com/", "https://github.com/explore",
        "https://stackoverflow.com/", "https://www.reddit.com/r/programming/"
    ]
    MasterNode(seeds).start()
    
# the end    
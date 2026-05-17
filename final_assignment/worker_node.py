import redis
import requests
import time
import json
import random
import threading
import re
import os
from urllib.parse import urljoin, urlparse
from config import *
from dijkstra import DijkstraScholtenManager
from middleware import MiddlewareManager
from database import DatabaseManager
from utils import Colors
from custom_queue import DistributedQueue
from rw_lock import DistributedRWLock
from semaphore import DistributedSemaphore

# WorkerNode είναι η κλάση που υλοποιεί τη λειτουργία ενός worker στο σύστημα. 
# Κάθε instance της κλάσης αυτής αντιπροσωπεύει έναν ξεχωριστό worker process που τρέχει και εκτελεί τις εργασίες crawling.

# κατι παει πολυ λαθος  .

class WorkerNode:
    """sumary_line
    
    Keyword arguments:
    argument -- description
    Return: return_description
    
    argument -- description
    Return: return_description
    
    """
    
    def __init__(self):
        # self.r εινα η σύνδεση με το Redis, που θα χρησιμοποιείται για όλες τις λειτουργίες που απαιτούν κοινόχρηστη κατάσταση μεταξύ των workers (π.χ. locks, queues, status updates).
        self.r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        #self.db είναι ο διαχειριστής της βάσης δεδομένων, που θα χρησιμοποιείται για να αποθηκεύει τα αποτελέσματα των crawled σελίδων και να ελέγχει αν ένα URL έχει ήδη επεξεργαστεί.
        self.db = DatabaseManager() 
        #self.ds είναι ο διαχειριστής του Dijkstra-Scholten αλγορίθμου, που θα χρησιμοποιείται για να διαχειρίζεται την κατανομή εργασίας και την ανίχνευση τερματισμού στο σύστημα.
        self.ds = DijkstraScholtenManager(self.r)
        #self.queue είναι η ουρά εργασιών που θα χρησιμοποιείται για να λαμβάνει URLs προς επεξεργασία. Είναι μια distributed queue που βασίζεται στο Redis, και επιτρέπει αξιόπιστο pop και acknowledge των εργασιών.
        # KEY_QUEUE είναι το Redis key που χρησιμοποιείται για την ουρά των URLs που πρέπει να επεξεργαστούν οι workers. Οι εργασίες θα μετακινούνται από αυτή την ουρά σε ένα προσωρινό key "crawler:processing:<worker_id>" όταν ένας worker τις αναλαμβάνει, και θα αφαιρούνται από εκεί όταν ολοκληρωθούν (acknowledged).
        self.queue = DistributedQueue(self.r, KEY_QUEUE)
        self.middleware = MiddlewareManager()
        self.rw_lock = DistributedRWLock(self.r)
        self.semaphore = DistributedSemaphore(self.r) # Dynamic Limit
        self.worker_id = f"W-{random.randint(100,999)}"
        self.processing_key = f"crawler:processing:{self.worker_id}"
        self.session = requests.Session()
        self.session.headers.update(self.middleware.get_headers())
        # Added 'depth' to stats for recovery context
        self.stats = {'done': 0, 'lat': 0, 'start': time.time(), 'role': 'IDLE', 'depth': 0}
        threading.Thread(target=self.heartbeat, daemon=True).start()
        
    #Η μέθοδος log είναι μια βοηθητική μέθοδος για την καταγραφή μηνυμάτων στο Redis, με timestamp και το worker_id για ευκολότερο debugging και παρακολούθηση της δραστηριότητας των workers.
    def log(self, msg):
        ts = time.strftime("[%H:%M:%S]")
        self.r.lpush("crawler:logs", f"{ts} {self.worker_id}: {msg}")
        self.r.ltrim("crawler:logs", 0, 99)
    #Η μέθοδος heartbeat είναι υπεύθυνη για την ενημέρωση της κατάστασης του worker στο Redis κάθε 0.5 δευτερόλεπτα.
    def heartbeat(self):
        while True:
            try:
                # --- LEADER ELECTION ---
                is_leader = False
                leader = self.r.get("crawler:leader")
                
                if not leader:
                    # Try to become leader
                    if self.r.set("crawler:leader", self.worker_id, ex=5, nx=True):
                        is_leader = True
                        self.log("I am the new LEADER!")
                elif leader == self.worker_id:
                    # Renew leadership
                    self.r.expire("crawler:leader", 5)
                    is_leader = True

                proxy = self.session.proxies.get('http') or 'Direct'
                if '@' in proxy: proxy = proxy.split('@')[1]

                # --- STATUS REPORT ---
                role_display = self.stats['role']
                if is_leader:
                    role_display = f"👑 LEADER ({role_display})"

                state = {
                    'id': self.worker_id, 
                    'status': role_display,
                    'jobs': self.stats['done'], 
                    'cpu': random.randint(5, 15),
                    'last_seen': time.time(), 
                    'current_url': self.stats.get('url', 'Idle'),
                    'depth': self.stats.get('depth', 0), # Critical for recovery
                    'proxy': proxy,
                    'is_leader': is_leader
                }
                
                # TTL increased to 60s so Master can detect timeout (15s) before Redis deletes the key
                self.r.setex(f"crawler:worker_status:{self.worker_id}", 60, json.dumps(state))

                if self.r.exists(f"crawler:kill:{self.worker_id}"): 
                    self.log("Received KILL signal. Bye!")
                    os._exit(0)
            except: pass
            time.sleep(0.5)
    # ---- ιδανικο σεναριο -- τωρα ειναι fake make up .., Να βρω να δω αμα μπορεις free proxiess το βλεπουμε         
    # Η μέθοδος rotate_identity είναι υπεύθυνη για την αλλαγή της ταυτότητας του worker,
    # δηλαδή την επιλογή ενός νέου User-Agent και ενός νέου Proxy από τη MiddlewareManager. 
    # Αυτό βοηθάει στο να μην μπλοκάρονται οι requests από τους servers που επισκέπτεται ο crawler, 
    # καθώς αλλάζει η ταυτότητα του worker σε κάθε 3 requests.        
    def rotate_identity(self):
        proxy = self.middleware.get_proxy()
        if proxy: self.session.proxies = {'http': proxy, 'https': proxy}
        else: self.session.proxies = {}
        self.session.headers.update(self.middleware.get_headers())
    # run είναι η κύρια μέθοδος που εκτελεί τον κύκλο εργασίας του worker.
    # Σε αυτό το loop, ο worker προσπαθεί να πάρει μια εργασία από την ουρά,
    # ελέγχει αν το URL έχει ήδη επεξεργαστεί, εφαρμόζει rate limiting, 
    # κάνει fetch το URL, αποθηκεύει τα αποτελέσματα στη βάση δεδομένων, 
    # και ανακαλύπτει νέα URLs για crawling.
    # Επίσης χειρίζεται σήματα τερματισμού και ενημερώνει την κατάσταση του worker στο Redis.
    def run(self):
        self.log("Online.") # Log initial status
        print(f"{Colors.GREEN}Worker {self.worker_id} Online.{Colors.ENDC}")
        self.rotate_identity() # Set initial identity
        request_count = 0 # Counter to track requests for identity rotation
        
        while True: # till we die or master kills us :P
            
            #SHUTDOWN SIGNAL HANDLER
            if self.r.get("crawler:shutdown") == "1": break
            # κανουμε pause το run loop αν το συστημα ειναι σε paused state, 
            # για να μην ξεκιναμε νεες εργασιες ενω ο master εχει πατησει pause,
            # αλλα να συνεχισουμε να επεξεργαζομαστε οτιδηποτε εχουμε ηδη αναλαβει
            if self.r.get("crawler:state:paused") == "1":
                self.stats['role'] = 'PAUSED'
                time.sleep(1)
                continue

            if request_count > 3:
                self.rotate_identity()
                request_count = 0
            # 'IDLE' role is the default state when the worker is waiting for new tasks. 
            # It helps to identify workers that are active but currently not processing any URLs, which can be useful for monitoring and debugging purposes.
            self.stats['role'] = 'IDLE'
            # USE RELIABLE POP
            # Moves item from QUEUE -> crawler:processing:<wid> atomically
            #safe_pop επιστρέφει None αν δεν υπάρχει item μέσα σε 2 δευτερόλεπτα, 
            # οπότε αν δεν πάρουμε item συνεχίζουμε το loop και ξαναδοκιμάζουμε.
            item = self.queue.safe_pop(self.processing_key, timeout=2)
            if not item: continue
            
            request_count += 1
            url = item.get('url') if isinstance(item, dict) else item
            
            # --- SHUTDOWN SIGNAL HANDLER ---
            if url == 'SHUTDOWN_SIGNAL':
                self.queue.acknowledge(self.processing_key, item)
                continue
            # -------------------------------

            depth = item.get('depth', 0) if isinstance(item, dict) else 0
            parent = item.get('parent', 'Master') # Default to Master
            
            self.stats['url'] = url
            self.stats['depth'] = depth
            domain = urlparse(url).netloc
            #Εδώ ξεκινάει το κύριο μέρος της εργασίας του worker, όπου προσπαθεί να επεξεργαστεί το URL που πήρε από την ουρά.
            try:
                # 1. READ DB CHECK (ΕΛΕΓΧΟΣ ΑΝΑΓΝΩΣΗΣ)
                self.stats['role'] = 'WAITING_READ_LOCK'
                self.log(f"Waiting for Read Lock ({url[:15]})...")
                #Εδώ προσπαθούμε να αποκτήσουμε το read lock πριν ελέγξουμε αν το URL υπάρχει ήδη στη βάση δεδομένων. 
                # Αυτό είναι σημαντικό για να διασφαλίσουμεότι δεν θα έχουμε race conditions όπου πολλοί workersελέγχουν ταυτόχρονα για το ίδιο URL και αποφασίζουν να το επεξεργαστούν επειδή δεν βλέπουν την ενημέρωση των άλλων workers στη βάση δεδομένων.
                if self.rw_lock.acquire_read(self.worker_id, timeout=5):
                    try:
                        # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                        self.stats['role'] = 'READING_DB' # αλαγη role για να φαινεται στο dashboard οτι διαβαζουμε απο τη βαση
                        self.log(f"Read Lock ACQUIRED ({url[:15]})...") 
                        time.sleep(1.0) # Visualization: Hold lock to be seen by Concurrency Tab
                        #Προσοχη εδω το check_url κανει query στη βαση για να δει αν το url υπαρχει ηδη, αν ναι επιστρεφει True και τοτε δεν χρειαζεται να το επεξεργαστουμε ξανα, απλα το αγνοουμε και παμε παρακατω. Αν δεν υπαρχει επιστρεφει False και τοτε προχωραμε στο να το επεξεργαστουμε κανονικα (fetch, save, κλπ).
                        # ο σημαιοφορος στο check_url ειναι ανα ενεργο worker , δηλαδη αν ενας worker εχει παρει read lock και τρεχει το check_url για ενα url, οι αλλοι workers που θα προσπαθησουν να παρουν read lock για το ιδιο url θα μπλοκαριστουν μεχρι να απελευθερωθει ο read lock, και αυτο βοηθαει στο να μην τρεχει πολλαπλοι workers το ιδιο url ταυτοχρονα, αλλα και στο να μην τρεχει καποιος worker το ιδιο url αν εχει ηδη αποθηκευτει στη βαση απο καποιον αλλο worker.
                        exists = self.db.check_url(url) # Ελέγχουμε αν το URL υπάρχει ήδη στη βάση δεδομένων. Αν ναι, δεν χρειάζεται να το επεξεργαστούμε ξανά.
                    finally:
                        self.rw_lock.release_read(self.worker_id)
                    
                    if exists:
                        self.log(f"Skipping {url[:30]}... (Exists)")
                        self.ds.ack_work(parent)
                        self.queue.acknowledge(self.processing_key, item)
                        continue
                else:
                    self.log(f"Read Lock Timeout on {url[:15]}...")
                    self.ds.ack_work(parent)
                    self.queue.acknowledge(self.processing_key, item)
                    continue
                
                # 1.5 RATE LIMITING (SEMAPHORE)
                self.stats['role'] = 'WAITING_TOKEN'
                if not self.semaphore.acquire(self.worker_id, timeout=10):
                    self.log("Token timeout. Skipping.")
                    self.ds.ack_work(parent)
                    self.queue.acknowledge(self.processing_key, item)
                    continue

                # 2. FETCH
                self.stats['role'] = 'FETCHING'
                try:
                    resp = self.session.get(url, timeout=5)
                except:
                    self.session.proxies = {}
                    resp = self.session.get(url, timeout=5)
                
                if resp.status_code == 200:
                    title_match = re.search(r'<title>(.*?)</title>', resp.text, re.I)
                    title = title_match.group(1)[:50] if title_match else domain
                    
                    # 3. WRITE DB
                    self.stats['role'] = 'WAITING_WRITE_LOCK'
                    self.log(f"Waiting for Write Lock ({url[:15]})...")
                    
                    if self.rw_lock.acquire_write(self.worker_id, timeout=60):
                        try:
                            # CRITICAL CHECK: Ensure no readers exist
                            readers_count = self.r.scard("rw:active_readers")
                            if readers_count > 0:
                                self.log(f"CRITICAL VIOLATION! Writing while {readers_count} Readers active!")

                            self.stats['role'] = 'SAVING_TO_DB'
                            self.log("Write Lock ACQUIRED. Saving...")
                            time.sleep(1.0) # Reduced delay to 1.0s
                            self.db.save_page(url, domain, title, 0.5, self.worker_id)
                            self.log("DB Saved.")
                        finally:
                            self.rw_lock.release_write(self.worker_id)
                            self.log("Releasing Write Lock.")
                    else:
                        self.log(f"Write Lock TIMEOUT on {url[:15]}...")
                    
                    self.stats['done'] += 1
                    res_data = {'url': url, 'worker': self.worker_id, 'domain': domain, 'depth': depth, 'title': title}
                    self.r.lpush(KEY_RESULTS, json.dumps(res_data))
                    self.r.ltrim(KEY_RESULTS, 0, 499)
                    
                    words = re.findall(r'\b[a-z]{5,15}\b', resp.text.lower())
                    for w in words[:20]: self.r.zincrby(KEY_KEYWORDS, 1, w)

                    max_d = int(self.r.get("crawler:config:depth") or 3)
                    if depth < max_d:
                        links = re.findall(r'href=["\'](http[s]?://.*?)["\']', resp.text)
                        for l in links[:5]:
                            if self.r.sadd(KEY_VISITED, l):
                                # Push with CURRENT worker as parent
                                self.queue.push({'url': l, 'depth': depth+1, 'parent': self.worker_id})
                                self.ds.send_work(self.worker_id, l)
                                self.r.lpush("crawler:graph_edges", json.dumps({'from': url, 'to': l}))
            except Exception as e:
                print(f"Error: {e}")
            
            # ACK actual parent
            self.ds.ack_work(parent)
            self.queue.acknowledge(self.processing_key, item)

if __name__ == "__main__":
    WorkerNode().run()
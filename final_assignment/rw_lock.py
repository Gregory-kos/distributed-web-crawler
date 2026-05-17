import time
import random
import redis
'''
χρησιμοποιούμε το DistributedRW για 
'''

class DistributedRWLock:
    def __init__(self, redis_conn):
        # self.r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        #Αν και θα μπορούσαμε να χρησιμοποιήσουμε ένα απλό threading.Lock για να προστατεύσουμε την πρόσβαση στη βάση δεδομένων,αυτό δεν θα ήταν αρκετό για να διαχειριστούμε τους concurrent readers και writers που μπορεί να υπάρχουν σε διαφορετικά processes (Flask + Workers).
        self.r = redis_conn
        # Χρησιμοποιούμε Redis για να διαχειριστούμε τους active readers και τον writer. Το lock είναι αυστηρό, δηλαδή δεν επιτρέπει νέους readers αν υπάρχει writer, και δεν επιτρέπει writer αν υπάρχουν active readers.
        self.key_readers = "rw:active_readers"
        self.key_writer = "rw:writer"

    def log(self, msg):
        try:
            #απλά για να έχουμε ένα log στο redis για debugging, με timestamp και το μήνυμα που θέλουμε να καταγράψουμε. Κρατάμε μόνο τα τελευταία 100 logs για να μην γεμίζει το redis.
            ts = time.strftime("[%H:%M:%S]")
            self.r.lpush("crawler:logs", f"{ts} RW_LOCK: {msg}")
            self.r.ltrim("crawler:logs", 0, 99)
        except: pass

    def acquire_read(self, worker_id, timeout=None):
        # STRICT READ: Only enter if NO WRITER exists.
        # το lua script ειναι για να κανει ολα τα check και το sadd ατομικα, για να μηνεχουμε race conditions sad
        # sadd προσθέτει το worker_id στο set των active readers, και το exists checkαρει αν υπαρχει writer, αν ναι δεν μπορω να παρω το read lock
        lua_script = """
        -- 1. Check if a Writer exists
        if redis.call("exists", KEYS[2]) == 1 then
            return 0
        end
        -- 2. Enter as Reader
        redis.call("sadd", KEYS[1], ARGV[1])
        return 1
        """
        
        start_time = time.time()
        while True:
            try:
                #self.log(f"{worker_id} attempting to acquire Read Lock...")# εδω κανουμε το lua script που κανει ολα τα check και το
                if self.r.eval(lua_script, 2, self.key_readers, self.key_writer, worker_id):
                    return True
            except Exception:
                pass
            
            if timeout and (time.time() - start_time) > timeout:
                return False
            time.sleep(0.05) # Fast retry
            
    #το release_read απλά αφαιρεί το worker_id από το set των active readers, δεν χρειάζεται να ελέγξουμε τίποτα άλλο γιατί αν υπάρχει writer δεν θα μπορούσε να έχει πάρει read lock εξαρχής
    def release_read(self, worker_id):
        try:
            #εδώ κάνουμε το srem που αφαιρεί το worker_id από το set των active readers, για να δηλώσουμε ότι αυτός ο worker δεν είναι πια reader. Αυτό είναι σημαντικό για να μπορούν οι writers να πάρουν το lock όταν δεν υπάρχουν active readers.
            self.r.srem(self.key_readers, worker_id)
        except: pass

    def acquire_write(self, worker_id, timeout=None):
        # STRICT WRITE: Only enter if NO READERS and NO WRITER.
        # το lua script ειναι για να κανει ολα τα check και το set ατομικα, για να μην εχουμε race conditions sad
        #  redis.call("set", KEYS[2], ARGV[1], "NX", "EX", 60) κανει set μονο αν δεν υπαρχει το key (NX) και βαζει expiration 60s για safety, αν επιστρεψει OK τοτε πηραμε το lock, αλλιως καποιος αλλος το εχει παρει
        #  η "scard" μετραει τους active readers, αν ειναι >0 δεν μπορω να παρω το write lock, και η "exists" checkαρει αν υπαρχει writer, αν ναι δεν μπορω να παρω το write lock
        # η "set" με NX και EX κανει το set μονο αν δεν υπαρχει το key, και βαζει expiration για safety, αν επιστρεψει OK τοτε πηραμε το lock, αλλιως καποιος αλλος το εχει παρει
        lua_script = """
        -- 1. Check for Readers (we want to be strict, so even 1 reader blocks the writer)
        if redis.call("scard", KEYS[1]) > 0 then
            return 0
        end
        -- 2. Check for Writer (we want to be strict, so if a writer exists, block)
        if redis.call("exists", KEYS[2]) == 1 then
            return 0
        end
        -- 3. Acquire Lock (60s TTL to be safe)
        -- We explicitly check if SET succeeds (returns OK)
        local res = redis.call("set", KEYS[2], ARGV[1], "NX", "EX", 60)
        if res then
            return 1
        else
            return 0
        end
        """
        
        start_time = time.time()
        last_log = 0
        while True:
            try:
                #self.log(f"{worker_id} attempting to acquire Write Lock...")
                # εδω κανουμε το lua script που κανει ολα τα check και το set ατομικα, για να μην εχουμε race conditions
                # το .eval επιστρεφει 1 αν πηραμε το lock, 0 αν δεν το πηραμε, οποτε αν ειναι 1 επιστρεφουμε True
                #self.key_readers ειναι το set με τους active readers, self.key_writer ειναι το key που δηλωνει αν υπαρχει writer, worker_id ειναι το id του worker που θελει να παρει το lock
                if self.r.eval(lua_script, 2, self.key_readers, self.key_writer, worker_id):
                    # self.log(f"{worker_id} acquired Write Lock")
                    return True
            except Exception:
                pass

            now = time.time()
            if timeout and (now - start_time) > timeout:
                self.log(f"{worker_id} Write Lock TIMEOUT")
                return False
            
            if now - last_log > 5:
                # self.log(f"{worker_id} waiting for Write Lock...")
                last_log = now
                
            time.sleep(0.1)

    def release_write(self, worker_id):
        # Only delete if I own it
        lua_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        try:
            # εδώ κάνουμε το lua script που ελέγχει αν ο τρέχων worker είναι αυτός που κατέχει το write lock, και αν ναι το απελευθερώνει, αλλιώς δεν κάνει τίποτα. Αυτό είναι για να αποφύγουμε να απελευθερώσει κάποιος άλλος worker το lock που δεν του ανήκει.
            self.r.eval(lua_script, 1, self.key_writer, worker_id)
        except: pass
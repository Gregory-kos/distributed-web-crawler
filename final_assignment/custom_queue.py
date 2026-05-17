import redis
import json
'''
DistributedQueue ειναι μια υλοποίηση της κατανεμημένης ουράς που χρησιμοποιεί Redis για την αποθήκευση και διαχείριση των εργασιών.
Παρέχει μεθόδους για την προσθήκη (push) και αφαίρεση (pop) εργασιών, καθώς και μια ασφαλή μέθοδο pop που εξασφαλίζει ότι οι εργασίες δεν χάνονται σε περίπτωση αποτυχίας του worker.
'''

class DistributedQueue:
    def __init__(self, redis_conn, key_name):
        # Initialize with a Redis connection and a key name for the queue , 
        # e.g., 'my_queue'  so that multiple instances can share the same queue.
        self.r = redis_conn # Redis connection (e.g., redis.Redis(host='localhost', port=6379, db=0))
        self.key = key_name # Key name for the queue in Redis (e.g., 'crawler:queue')

    def push(self, item):
        """Add item to the queue"""
        # Serialize if dict
        if isinstance(item, dict):
            item = json.dumps(item)
        self.r.lpush(self.key, item)

    def pop(self, timeout=0):
        # helper method to pop an item from the queue. It uses the Redis BRPOP command, which is a blocking pop operation.
        """Remove and return an item from the queue (Blocking). Auto-decodes JSON."""
        # Returns (key, value) tuple from brpop, we only need value
        item = self.r.brpop(self.key, timeout=timeout)
        if item:
            val = item[1]
            try:
                return json.loads(val)
            except (json.JSONDecodeError, TypeError):
                return val
        return None
    # safe_pop είναι μια μέθοδος που χρησιμοποιεί την εντολή RPOPLPUSH για να μετακινήσει ένα στοιχείο από την ουρά σε μια λίστα επεξεργασίας, 
    # εξασφαλίζοντας ότι το στοιχείο δεν θα χαθεί αν ο worker αποτύχει κατά την επεξεργασία του.
    # Η μέθοδος αυτή επιστρέφει το στοιχείο που μετακινήθηκε ή None αν δεν υπάρχει διαθέσιμο στοιχείο.
    # διαφορα με pop ειναι οτι το pop αφαιρει το στοιχειο απο την ουρα και το επιστρεφει, ενω το safe_pop μετακιναει το στοιχειο 
    # σε μια λιστα επεξεργασιας και επιστρεφει το στοιχειο χωρις να το αφαιρει απο την ουρα, ετσι αν ο worker αποτυχει 
    # το στοιχειο δεν χαθηκε και μπορει να επεξεργαστει απο αλλον worker.
    def safe_pop(self, dest_key, timeout=0):
        """
        Reliable Pop: Atomically pops from queue and pushes to dest_key (processing list).
        Returns the item or None.
        """
        item = self.r.brpoplpush(self.key, dest_key, timeout=timeout)
        if item:
            try:
                return json.loads(item)
            except (json.JSONDecodeError, TypeError):
                return item # Return raw string/bytes if not JSON
        return None
    # acknowledge είναι μια μέθοδος που αφαιρεί ένα στοιχείο από τη λίστα επεξεργασίας, 
    # υποδεικνύοντας ότι η επεξεργασία του έχει ολοκληρωθεί.
    def acknowledge(self, processing_key, item):
        """
        Removes the item from the processing list, signifying completion.
        """
        # Serialize if dict to match what's in Redis
        if isinstance(item, dict):
            item = json.dumps(item)
        self.r.lrem(processing_key, 1, item)

    def size(self):
        """Return current size of the queue"""
        return self.r.llen(self.key)

    def clear(self):
        """Empty the queue"""
        self.r.delete(self.key)
        
        
        
        
'''
να μην ξεχασω 
Η custom ουρά μας χρησιμοποιεί τη Redis για να είναι προσβάσιμη από όλους τους Workers ταυτόχρονα. Το κυριότερο
  χαρακτηριστικό της είναι η αξιοπιστία: χρησιμοποιούμε την εντολή `safe_pop` η οποία μεταφέρει την εργασία σε μια προσωρινή
  λίστα 'επεξεργασίας'. Έτσι, αν ένας Worker διακοπεί απότομα, η εργασία δεν χάνεται, αλλά παραμένει καταγεγραμμένη μέχρι να
  γίνει `acknowledge`

'''        
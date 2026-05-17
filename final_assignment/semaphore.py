import time

class DistributedSemaphore:
    def __init__(self, redis_conn):
        #Χρησιμοποιούμε Redis για να διαχειριστούμε το token bucket που λειτουργεί ως distributed semaphore για τον έλεγχο του ρυθμού των requests.
        self.r = redis_conn
        #Τα keys που χρησιμοποιούμε στο Redis για να αποθηκεύσουμε τον αριθμό των διαθέσιμων tokens και το timestamp του τελευταίου refill. Αυτά τα keys θα χρησιμοποιούνται από το Lua script για να διαχειριστεί το token bucket.
        self.key_tokens = "crawler:semaphore:tokens"
        self.key_last = "crawler:semaphore:last_refill"
    # Η μέθοδος acquire προσπαθεί να αποκτήσει το semaphore, 
    # δηλαδή να πάρει ένα token από το bucket. Χρησιμοποιεί ένα Lua script για να διαχειριστεί το token bucket με ακρίβεια,
    # λαμβάνοντας υπόψη τον ρυθμό (rate) και την χωρητικότητα (capacity) του bucket.
    # Αν υπάρχουν διαθέσιμα tokens, τα αφαιρεί και επιστρέφει True, διαφορετικά επιστρέφει False.
    # Η μέθοδος αυτή μπορεί να χρησιμοποιηθεί από τους workers για να ελέγχουν τον ρυθμό των requests που κάνουν προς τους servers.
    def acquire(self, worker_id, timeout=10):
        start = time.time()
        while True:
            # 1. Get Dynamic Limit (Rate)
            try:
                rate = float(self.r.get("crawler:config:rate_limit") or 10)
            except: rate = 10.0
            
            # Capacity usually equals rate (1 second burst)
            capacity = rate 

            # 2. Lua Script (Token Bucket with Floats)
            # Uses floating point math to prevent losing partial tokens
            lua_script = """
            local rate = tonumber(ARGV[1])
            local capacity = tonumber(ARGV[2])
            local now = tonumber(ARGV[3])
            local requested = 1
            
            local tokens = tonumber(redis.call("get", KEYS[1]))
            local last_refill = tonumber(redis.call("get", KEYS[2]))
            
            -- Initialize
            if not tokens then tokens = capacity end
            if not last_refill then last_refill = now end
            
            -- Refill Logic (Exact Math)
            local delta = math.max(0, now - last_refill)
            local filled_tokens = delta * rate
            tokens = math.min(capacity, tokens + filled_tokens)
            
            -- Update Timestamp (Always, because we accounted for the delta in tokens)
            redis.call("set", KEYS[2], now)
            
            -- Acquire Logic
            if tokens >= requested then
                tokens = tokens - requested
                redis.call("set", KEYS[1], tokens)
                return 1 -- Granted
            else
                redis.call("set", KEYS[1], tokens)
                return 0 -- Denied
            end
            """
            
            try:
                # Keys: [Tokens, Timestamp]
                # Args: [Rate, Capacity, Now]
                res = self.r.eval(lua_script, 2, self.key_tokens, self.key_last, rate, capacity, time.time())
                
                if res == 1:
                    return True
            except Exception as e:
                print(f"Semaphore Logic Error: {e}")

            if time.time() - start > timeout:
                return False
            
            # Sleep slightly to prevent CPU spinning, but fast enough to catch tokens
            time.sleep(0.1)
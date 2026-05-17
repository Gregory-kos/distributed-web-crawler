import sqlite3
import threading
import os

# Βρίσκουμε το μονοπάτι του φακέλου όπου βρίσκεται ΑΥΤΟ το αρχείο (final_assignment)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_NAME = os.path.join(BASE_DIR, "crawler.db")

class DatabaseManager:
    def __init__(self, reset=False):
        self.lock = threading.Lock()
        if reset and os.path.exists(DB_NAME):
            try: os.remove(DB_NAME)
            except: pass
        self.init_db()


    """ 
    parameters:
    - reset: αν True, διαγράφει το υπάρχον DB αρχείο (αν υπάρχει) και δημιουργεί ένα νέο. Χρήσιμο για καθαρό ξεκίνημα.
    - check_same_thread=False: επιτρέπει τη χρήση της ίδιας σύνδεσης από διαφορετικά threads (Flask + Workers). 
    Εναλλακτικά, θα μπορούσαμε να δημιουργούμε μια νέα σύνδεση για κάθε λειτουργία, 
    αλλά αυτό μπορεί να είναι πιο αργό. Με το lock εξασφαλίζουμε ότι οι concurrent προσβάσεις δεν θα προκαλέσουν προβλήματα.
    
    Η βάση δεδομένων έχει ένα πίνακα "pages" με τα πεδία: id, url, domain, title, time, worker.
    Παρέχονται μέθοδοι για αποθήκευση σελίδας, έλεγχο ύπαρξης URL, αναζήτηση με φίλτρα, και στατιστικά.
    
    
    """
    def init_db(self):
        with self.lock:
            # check_same_thread=False για να επιτρέπεται η χρήση από Flask και Workers ταυτόχρονα
            conn = sqlite3.connect(DB_NAME, check_same_thread=False)
            c = conn.cursor()
            c.execute('''CREATE TABLE IF NOT EXISTS pages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE,
                domain TEXT,
                title TEXT,
                time REAL,
                worker TEXT
            )''')
            conn.commit()
            conn.close()

    """ 
    - save_page(url, domain, title, time_taken, worker): αποθηκεύει τα στοιχεία μιας σελίδας στη βάση.
     - url, domain, title, time_taken, worker: τα στοιχεία της σελίδας που θα αποθηκευτούν στη βάση. Το url είναι μοναδικό (UNIQUE) για να αποφεύγονται διπλοεγγραφές.
     - check_url(url): Επιστρέφει True αν το URL υπάρχει ήδη στη βάση, False αν όχι. Χρησιμοποιείται για να αποφύγουμε την επεξεργασία του ίδιου URL πολλές φορές.
     - search_pages(query): Εκτελεί αναζήτηση στη βάση με βάση το query. Υποστηρίζει ειδικά φίλτρα όπως "site:domain.com" 
    """

    def save_page(self, url, domain, title, time_taken, worker):
        with self.lock:
            try:
                conn = sqlite3.connect(DB_NAME, timeout=10)
                c = conn.cursor()
                c.execute("INSERT OR IGNORE INTO pages (url, domain, title, time, worker) VALUES (?,?,?,?,?)",
                          (url, domain, title, time_taken, worker))
                conn.commit()
                conn.close()
                return True
            except Exception as e:
                print(f"DB Error: {e}")
                return False

    # Επιστρέφει True αν το URL υπάρχει ήδη στη βάση, False αν όχι. Χρησιμοποιείται για να αποφύγουμε την επεξεργασία του ίδιου URL πολλές φορές.
    def check_url(self, url):
       
        # αυτο ειναι για να προσταυσουμε τον ιδιο  worker να μην τρέχει 2 φορές το ιδιο url ταυτόχρονα, αλλα και για να μην τρέχει κανένας άλλος worker το ίδιο url αν έχει ήδη αποθηκευτεί στη βάση
        with self.lock:
            
            try:
                conn = sqlite3.connect(DB_NAME, timeout=5)
                c = conn.cursor()
                c.execute("SELECT 1 FROM pages WHERE url = ?", (url,))
                exists = c.fetchone() is not None
                conn.close()
                return exists
            except: return False

    def search_pages(self, query):
        results = []
        try:
            conn = sqlite3.connect(DB_NAME, timeout=10.0)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            
            # --- ADVANCED SEARCH PARSING ---
            # Supports: "site:github.com python" or "worker:W-105 api"
            parts = query.split()
            search_terms = []
            domain_filter = None
            worker_filter = None
            
            for p in parts:
                if p.lower().startswith("site:"):
                    domain_filter = p.split(":", 1)[1]
                elif p.lower().startswith("worker:"):
                    worker_filter = p.split(":", 1)[1]
                else:
                    search_terms.append(p)
            
            # Build SQL dynamically
            sql = "SELECT title, url, worker, time, domain FROM pages WHERE 1=1"
            params = []
            
            # Apply Filters
            if domain_filter:
                sql += " AND domain LIKE ?"
                params.append(f'%{domain_filter}%')
            
            if worker_filter:
                sql += " AND worker LIKE ?"
                params.append(f'%{worker_filter}%')
            
            # Apply Keyword Search (AND logic for each word)
            for term in search_terms:
                sql += " AND (title LIKE ? OR url LIKE ?)"
                params.extend([f'%{term}%', f'%{term}%'])
            
            # Sort by newest first
            sql += " ORDER BY id DESC LIMIT 50"
            
            c.execute(sql, params)
            rows = c.fetchall()
            results = [dict(row) for row in rows]
            conn.close()
        except Exception as e:
            print(f"Search Error: {e}")
        return results

    def get_worker_efficiency(self):
        results = []
        try:
            conn = sqlite3.connect(DB_NAME, timeout=5.0)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            c.execute("SELECT worker, COUNT(*) as cnt FROM pages GROUP BY worker ORDER BY cnt DESC")
            rows = c.fetchall()
            results = [dict(row) for row in rows]
            conn.close()
        except: pass
        return results

    def get_stats(self):
        stats = {'total': 0, 'db_size': 0, 'avg_time': 0, 'top_worker': '-'}
        try:
            conn = sqlite3.connect(DB_NAME, timeout=1.0)
            c = conn.cursor()
            
            # 1. Total Pages
            c.execute("SELECT COUNT(*) FROM pages")
            stats['total'] = c.fetchone()[0]
            
            # 2. Avg Time
            c.execute("SELECT AVG(time) FROM pages")
            avg = c.fetchone()[0]
            stats['avg_time'] = round(avg, 3) if avg else 0
            
            # 3. Top Worker
            c.execute("SELECT worker, COUNT(*) as cnt FROM pages GROUP BY worker ORDER BY cnt DESC LIMIT 1")
            row = c.fetchone()
            if row:
                stats['top_worker'] = f"{row[0]} ({row[1]})"
            
            conn.close()
            
            # 4. DB File Size (KB)
            if os.path.exists(DB_NAME):
                stats['db_size'] = round(os.path.getsize(DB_NAME) / 1024, 2)
                
        except: pass
        return stats

    def get_recent(self, limit=50):
        try:
            conn = sqlite3.connect(DB_NAME)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            c.execute("SELECT * FROM pages ORDER BY id DESC LIMIT ?", (limit,))
            rows = [dict(row) for row in c.fetchall()]
            conn.close()
            return rows
        except: return []
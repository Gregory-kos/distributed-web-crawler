# DatabaseManager Documentation (`database.py`)

## Σκοπός Αρχείου
Διαχειρίζεται τη σύνδεση με την τοπική βάση δεδομένων **SQLite**. Παρόλο που η SQLite είναι file-based, η χρήση της σε περιβάλλον πολλαπλών νημάτων/διεργασιών απαιτεί προσοχή.

## Κύριες Λειτουργίες

### `__init__` & `init_db`
*   Δημιουργεί τον πίνακα `pages` αν δεν υπάρχει.
*   Χρησιμοποιεί `check_same_thread=False` για να επιτρέπει τη χρήση από το Flask.

### `save_page(...)`
*   Αποθηκεύει τα metadata της σελίδας (URL, Title, Worker, Time).
*   Προστατεύεται από `threading.Lock` (σε επίπεδο process) και από το Distributed RW-Lock (σε επίπεδο συστήματος) που καλείται από τον Worker.

### `search_pages(self, query)`
*   Εκτελεί αναζήτηση στη βάση.
*   **Smart Search:** Υποστηρίζει φίλτρα όπως `site:domain.com` και `worker:ID` αναλύοντας το query string.

### `get_worker_efficiency(self)`
*   Εκτελεί `GROUP BY worker` query για να βρει πόσες σελίδες έχει κατεβάσει ο κάθε worker.
*   Χρησιμοποιείται για το Efficiency Table στο Dashboard.

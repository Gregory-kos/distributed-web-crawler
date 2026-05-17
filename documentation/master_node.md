# MasterNode Documentation (`master_node.py`)

## Σκοπός Αρχείου
Ο `MasterNode` είναι ο "εγκέφαλος" του κατανεμημένου συστήματος. Δεν εκτελεί crawling ο ίδιος, αλλά συντονίζει τους Workers, διαχειρίζεται την αρχικοποίηση (seeding) και επιβλέπει την υγεία του συστήματος.

## Κύριες Λειτουργίες

### `__init__(self, seeds)`
*   Συνδέεται με τη Redis.
*   Καθαρίζει τη βάση δεδομένων και το Redis (αν είναι νέα εκκίνηση).
*   Αρχικοποιεί τους διαχειριστές (`DijkstraScholtenManager`, `DistributedQueue`, `DatabaseManager`).

### `start(self)`
*   **Seeding:** Τοποθετεί τα αρχικά URLs στην ουρά.
*   **Initialization:** Θέτει το αρχικό Deficit του Master ίσο με τον αριθμό των seeds (για τον αλγόριθμο Dijkstra).
*   **Threads:** Ξεκινάει τα threads παρακολούθησης (`monitor_termination`, `monitor_fleet`).

### `monitor_fleet(self)` - **ΚΡΙΣΙΜΟ**
Αυτή η μέθοδος υλοποιεί την **Ανοχή Σφαλμάτων (Fault Tolerance)**.
*   Ελέγχει κάθε 5 δευτερόλεπτα τα heartbeats των workers.
*   Αν ένας worker δεν έχει δώσει σήμα για >15 δευτερόλεπτα, θεωρείται νεκρός.
*   **Reliable Recovery:** Ελέγχει τη λίστα `crawler:processing:<wid>` στο Redis για να βρει ποια εργασία έμεινε στη μέση.
*   **Re-queue:** Ξαναβάζει την εργασία στην ουρά διατηρώντας τον αρχικό "Parent".
*   **Atomic Deficit Transfer:** Καλεί την `self.ds.transfer_deficit` (Lua Script) για να μεταφέρει το χρέος του νεκρού worker στον Master, διασφαλίζοντας τη μαθηματική ορθότητα του αλγορίθμου τερματισμού.

### `monitor_termination(self)`
*   Ρωτάει τον `DijkstraScholtenManager` αν το σύστημα τερμάτισε.
*   Συνθήκη τερματισμού: `Total Deficit == 0` και η ουρά είναι άδεια.

### `heartbeat(self)`
*   Δηλώνει την παρουσία του Master στο Redis ώστε να φαίνεται στο Dashboard.

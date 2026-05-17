# WorkerNode Documentation (`worker_node.py`)

## Σκοπός Αρχείου
Ο `WorkerNode` είναι η μονάδα εργασίας. Λειτουργεί αυτόνομα, ανταγωνίζεται για πόρους και εκτελεί το βαρύ έργο του crawling.

## Κύριες Λειτουργίες

### `run(self)` - Ο Κύριος Βρόχος (Main Loop)
Αυτή είναι η καρδιά του Worker. Εκτελείται ατέρμονα μέχρι να ληφθεί σήμα τερματισμού.

1.  **Reliable Pop (`queue.safe_pop`)**:
    *   Ανακτά ένα URL από την ουρά.
    *   Χρησιμοποιεί `BRPOPLPUSH` (μέσω του `custom_queue`) για να μεταφέρει ατομικά το URL σε μια λίστα επεξεργασίας (`crawler:processing:<id>`). Αυτό διασφαλίζει ότι αν ο worker κλείσει, η εργασία δεν χάνεται.

2.  **Concurrency Control (Locks & Semaphores)**:
    *   **RW-Lock (Read):** Κλειδώνει για ανάγνωση (`acquire_read`) για να ελέγξει αν το URL υπάρχει ήδη στη DB.
    *   **Semaphore:** Ζητάει "Token" (`acquire`) για να σεβαστεί το Rate Limit.

3.  **Crawling**:
    *   Κατεβάζει τη σελίδα (HTTP GET).
    *   Εξάγει τίτλο και keywords.

4.  **Critical Section (Write)**:
    *   **RW-Lock (Write):** Κλειδώνει αποκλειστικά (`acquire_write`) για να γράψει τα αποτελέσματα στη βάση SQLite. Κανείς άλλος δεν μπορεί να διαβάζει ή να γράφει εκείνη τη στιγμή.

5.  **Dijkstra Signaling**:
    *   **Send Work:** Για κάθε νέο link που βρίσκει, στέλνει σήμα (`send_work`) αυξάνοντας το δικό του Deficit.
    *   **Ack Work:** Όταν τελειώσει την τρέχουσα σελίδα, στέλνει επιβεβαίωση (`ack_work`) στον Parent που του έδωσε τη δουλειά.

### `heartbeat(self)`
*   Στέλνει κάθε 0.5s ένα JSON στο Redis με την κατάστασή του (CPU, Current URL, Status).
*   Αυτό επιτρέπει στον Master να ξέρει ότι ο worker είναι ζωντανός.

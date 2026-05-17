# DistributedRWLock Documentation (`rw_lock.py`)

## Σκοπός Αρχείου
Υλοποιεί ένα **Κατανεμημένο Readers-Writers Lock** (RW-Lock) χρησιμοποιώντας Redis. Επιτρέπει πολλαπλούς ταυτόχρονους αναγνώστες, αλλά αποκλειστική πρόσβαση στον εγγραφέα.

## Λογική Λειτουργίας

*   **Writers Preference:** (Σε αυτή την απλοποιημένη υλοποίηση, δίνεται προτεραιότητα στην ασφάλεια εγγραφής).
*   **Redis Keys:**
    *   `rw:writer`: Κρατάει το ID του τρέχοντος Writer (αν υπάρχει).
    *   `rw:active_readers`: Ένα Redis Set με τα ID των ενεργών Readers.

## Κύριες Μέθοδοι

### `acquire_read(self, worker_id, timeout=None)`
*   Προσπαθεί να αποκτήσει κλείδωμα ανάγνωσης.
*   **Condition:** Επιτρέπεται ΜΟΝΟ αν δεν υπάρχει ενεργός Writer (`NOT EXISTS rw:writer`).
*   Αν επιτραπεί, προσθέτει τον εαυτό του στο Set `rw:active_readers`.

### `release_read(self, worker_id)`
*   Αφαιρεί τον εαυτό του από το `rw:active_readers`.

### `acquire_write(self, worker_id, timeout=None)`
*   Προσπαθεί να αποκτήσει αποκλειστικό κλείδωμα εγγραφής.
*   **Condition:** Επιτρέπεται ΜΟΝΟ αν:
    1.  Δεν υπάρχει άλλος Writer.
    2.  Δεν υπάρχουν ενεργοί Readers (`SCARD rw:active_readers == 0`).
*   Χρησιμοποιεί `SETNX` (Set if Not Exists) για ατομικότητα.

### `release_write(self, worker_id)`
*   Διαγράφει το κλειδί `rw:writer` ΜΟΝΟ αν κατέχεται από τον ίδιο τον worker.

# Ανάλυση Κώδικα: `dijkstra.py`

Αυτό το αρχείο υλοποιεί τον αλγόριθμο **Dijkstra-Scholten** για την ανίχνευση τερματισμού (Termination Detection) σε ένα κατανεμημένο σύστημα. Σκοπός του είναι να απαντήσει στο ερώτημα: *"Τελείωσε η δουλειά ή υπάρχει ακόμα κάποιος worker που επεξεργάζεται κάτι;"*

## Βασική Έννοια: Το "Έλλειμμα" (Deficit)
Η κεντρική ιδέα είναι η διατήρηση ενός ισοζυγίου εργασιών.
*   **Χρέωση (+1):** Όταν ένας κόμβος στέλνει μια εργασία.
*   **Πίστωση (-1):** Όταν μια εργασία ολοκληρώνεται (ACK).

Όταν το συνολικό άθροισμα (Deficit) γίνει **0**, το σύστημα έχει τερματίσει.

---

## Ανάλυση Κλάσης `DijkstraScholtenManager`

Η κλάση διαχειρίζεται την κατάσταση του αλγορίθμου χρησιμοποιώντας τη **Redis** ως κεντρική μνήμη.

### 1. `__init__(self, redis_conn)`
Αρχικοποιεί τον manager συνδέοντάς τον με τη Redis.
*   `redis_conn`: Η ενεργή σύνδεση με τη βάση Redis.

### 2. `reset(self)`
Καθαρίζει την κατάσταση για μια νέα εκτέλεση.
```python
self.r.delete(KEY_DS_DEFICIT, KEY_DS_GRAPH)
self.r.hset(KEY_DS_DEFICIT, "Master", 0)
```
*   Διαγράφει τα παλιά δεδομένα (`KEY_DS_DEFICIT`).
*   Ορίζει το "Master" με έλλειμμα 0 (σημείο εκκίνησης).

### 3. `send_work(self, sender_id, task_id)`
Καλείται όταν ένας Worker (ο `sender_id`) δημιουργεί μια νέα εργασία (βρίσκει ένα URL).
```python
self.r.hincrby(KEY_DS_DEFICIT, sender_id, 1)
```
*   **Λειτουργία:** Αυξάνει το έλλειμμα του `sender_id` κατά **1**.
*   **Σημασία:** Ο `sender_id` δηλώνει υπεύθυνος για αυτό το URL μέχρι να ολοκληρωθεί.

### 4. `ack_work(self, parent_id)`
Καλείται όταν ένας Worker ολοκληρώσει την επεξεργασία ενός URL.
```python
self.r.hincrby(KEY_DS_DEFICIT, parent_id, -1)
```
*   **Λειτουργία:** Μειώνει το έλλειμμα του `parent_id` (του γονέα που δημιούργησε την εργασία) κατά **1**.
*   **Προσοχή:** Δεν μειώνει το έλλειμμα αυτού που έκανε τη δουλειά, αλλά αυτού που την *ανέθεσε*. Αυτό κλείνει τον κύκλο.

### 5. `get_status(self)`
Επιστρέφει τη συνολική εικόνα του συστήματος.
```python
total_deficit = sum(clean_deficits.values())
is_terminated = (total_deficit == 0) and (len(clean_deficits) > 0)
```
*   Διαβάζει όλα τα ελλείμματα από τη Redis.
*   Αθροίζει τις τιμές.
*   Αν το άθροισμα είναι **0**, το σύστημα θεωρείται **Terminated**.

### 6. `transfer_deficit(self, from_id, to_id)`
Μια κρίσιμη λειτουργία για την ανθεκτικότητα (Fault Tolerance).
```lua
local val = redis.call('HGET', KEYS[1], ARGV[1])
if val then
    redis.call('HINCRBY', KEYS[1], ARGV[2], val) -- Μεταφορά στον νέο
    redis.call('HDEL', KEYS[1], ARGV[1])         -- Διαγραφή παλιού
    return val
end
```
*   **Σενάριο:** Αν ένας Worker "πεθάνει" (crash) ενώ έχει θετικό έλλειμμα (περιμένει απαντήσεις), το σύστημα δεν θα τερματίσει ποτέ (το έλλειμμα δεν θα μηδενίσει).
*   **Λύση:** Ο Master καλεί αυτή τη συνάρτηση για να μεταφέρει το "χρέος" του νεκρού Worker στον εαυτό του (ή σε άλλον). Έτσι, τα ACKs που θα έρθουν αργότερα θα μειώσουν το έλλειμμα του Master και το ισοζύγιο θα διατηρηθεί σωστό.
*   Χρησιμοποιεί **Lua Script** για να γίνει η μεταφορά ατομικά (atomic), ώστε να μην χαθούν δεδομένα αν συμβεί κάτι την ίδια στιγμή.

---

## Παράδειγμα Ροής

1.  **Start:** Master (Deficit: 0).
2.  **Master:** Βρίσκει το `google.com` -> Στέλνει στον Worker A.
    *   `send_work('Master')` -> Master Deficit: **1**.
3.  **Worker A:** Επεξεργάζεται το `google.com`. Βρίσκει το `gmail.com`.
    *   `send_work('Worker A')` -> Worker A Deficit: **1**.
    *   Στέλνει το `gmail.com` στον Worker B.
4.  **Worker A:** Τελειώνει με το `google.com`.
    *   `ack_work('Master')` -> Master Deficit: **0**.
5.  **Worker B:** Τελειώνει με το `gmail.com`.
    *   `ack_work('Worker A')` -> Worker A Deficit: **0**.

**Τέλος:** Master (0) + Worker A (0) = **0**. Το σύστημα τερματίζει.

# DistributedSemaphore Documentation (`semaphore.py`)

## Σκοπός Αρχείου
Υλοποιεί έναν **Κατανεμημένο Σηματοφορέα (Semaphore)** για Rate Limiting. Λειτουργεί ως μηχανισμός "Token Bucket".

## Λογική Λειτουργίας
*   Υπάρχει μια δεξαμενή με "μάρκες" (tokens).
*   Ο αριθμός των tokens ανανεώνεται αυτόματα με τον χρόνο.
*   Για να κάνει request, ένας worker πρέπει να πάρει μια μάρκα.

## Κύριες Μέθοδοι

### `acquire(self, worker_id, timeout=10)`
*   Ελέγχει αν υπάρχουν διαθέσιμα tokens.
*   **Refill Logic:** Υπολογίζει πόσα tokens πρέπει να προστεθούν βάσει του χρόνου που πέρασε από την τελευταία ανανέωση και του ρυθμού (`rate_limit`).
*   Αν `tokens >= 1`, μειώνει τα tokens κατά 1 και επιστρέφει `True`.
*   Όλη η διαδικασία (Read -> Calculate -> Write) γίνεται μέσα σε **Lua Script** για να είναι ατομική και thread-safe.

### `set_limit(self, limit)`
*   Αλλάζει δυναμικά τον μέγιστο ρυθμό (Requests Per Second) μέσω του Dashboard.

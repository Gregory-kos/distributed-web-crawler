# DashboardApp Documentation (`dashboard_app.py`)

## Σκοπός Αρχείου
Μια εφαρμογή ιστού (Web Application) βασισμένη στο **Flask** που παρέχει:
1.  Οπτικοποίηση της κατάστασης του συστήματος σε πραγματικό χρόνο.
2.  Διεπαφή ελέγχου (Control Panel) για τον χρήστη.
3.  API Endpoints για επικοινωνία με το Frontend.

## Κύριες Λειτουργίες

### API Endpoints
*   `/api/data`: Επιστρέφει ένα τεράστιο JSON με όλη την κατάσταση:
    *   Stats (Queue size, Total pages, PPS).
    *   Active Workers (Last seen, Status, Current URL).
    *   Dijkstra Deficits.
    *   Logs.
    *   Locks status.
*   `/api/control/kill`: Επιτρέπει στον χρήστη να σκοτώσει έναν worker (για test).
*   `/api/control/chaos`: Ενεργοποιεί/Απενεργοποιεί το Chaos Mode.

### Chaos Mode (`chaos_monkey`)
*   Ένα background thread που τρέχει παράλληλα με το Flask.
*   Αν είναι ενεργό (`chaos_mode_active = True`), επιλέγει τυχαία έναν worker (εκτός του Master) και τον "σκοτώνει" (στέλνει σήμα kill μέσω Redis).
*   Χρησιμοποιείται για να επιδείξει την ανθεκτικότητα του συστήματος (Fault Tolerance Demo).

### `db_stats()`
*   Επιστρέφει στατιστικά από την SQL βάση, συμπεριλαμβανομένου του "Worker Efficiency".

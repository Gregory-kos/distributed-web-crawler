# Config & Middleware Documentation

## `config.py`
Περιέχει τις καθολικές ρυθμίσεις του συστήματος.
*   **Redis Config:** Host, Port, DB.
*   **Redis Keys:** Κεντρικός ορισμός των ονομάτων κλειδιών (`crawler:queue`, `crawler:results`, κλπ) για αποφυγή hardcoding strings στον κώδικα.

## `middleware.py`
Διαχειρίζεται την "ταυτότητα" του Crawler προς τα έξω.
*   **User-Agent Rotation:** Επιλέγει τυχαίο User-Agent για κάθε request ώστε να αποφύγει το μπλοκάρισμα.
*   **Proxy Management:** (Προσομοίωση) Επιλέγει τυχαία proxy IPs για να φαίνεται ότι τα αιτήματα έρχονται από διαφορετικές τοποθεσίες.

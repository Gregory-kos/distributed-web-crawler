# Χρησιμοποιούμε μια ελαφριά εικόνα Python
FROM python:3.9-slim-buster

# Ορίζουμε τον φάκελο εργασίας μέσα στο container
WORKDIR /app

# Αντιγράφουμε πρώτα το requirements.txt για να εκμεταλλευτούμε το Docker cache
COPY requirements.txt requirements.txt

# Εγκαθιστούμε τις βιβλιοθήκες
RUN pip install --no-cache-dir -r requirements.txt

# Αντιγράφουμε όλο τον υπόλοιπο κώδικα της εφαρμογής
COPY . .

# Η εντολή που θα τρέχει by default (μπορεί να γίνει override)
CMD ["python", "master_node.py"]

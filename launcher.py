import subprocess
import sys
import os
import time

# να μην ξεχασω
# η python εχει εναν μηχανισμο που ονομαζεται GIL (Global Interpreter Lock) που επιτρέπει μόνο σε ένα thread 
# να εκτελείται τη φορά. Αυτό σημαίνει ότι ακόμα και αν έχουμε πολλαπλά threads, 
# μόνο ένα μπορεί να εκτελείται ενεργά, και τα άλλα είναι σε κατάσταση αναμονής. 
# Αυτό μπορεί να οδηγήσει σε περιορισμένη απόδοση όταν προσπαθούμε να κάνουμε CPU-bound εργασίες με threads. 
# Για να ξεπεράσουμε αυτό το πρόβλημα, μπορούμε να χρησιμοποιήσουμε multiprocessing αντί για threading, 
# καθώς το multiprocessing δημιουργεί ξεχωριστές διεργασίες που μπορούν να εκτελούνται παράλληλα χωρίς να επηρεάζονται από το GIL.
#-------------- καλο ειναι γενικα να αποφεύγουμε να χρησιμοποιούμε threading για CPU-bound εργασίες στην Python απο wiki
# Paths

#  μπορουμε να το τρεξουμε απο εδω η απο το docker 
# επισης μπορουμε να τρεξουμε το master και το dashboard ξεχωριστα αν θελουμε, απλα θα πρεπει να τρεξουμε πρωτα το dashboard για να ειναι ετοιμο οταν ξεκινησει ο master

#Αυτό το launcher είναι υπεύθυνο για την εκκίνηση του συστήματος crawler, συμπεριλαμβανομένων του master node και του dashboard.

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MASTER_SCRIPT = os.path.join(BASE_DIR, "final_assignment", "master_node.py")
DASH_SCRIPT = os.path.join(BASE_DIR, "final_assignment", "dashboard_app.py")
PY_EXE = sys.executable

print(f" LAUNCHING CRAWLER SYSTEM")
print(f" Project Base: {BASE_DIR}")
print(f" Python: {PY_EXE}")

if os.name == 'nt':
    CREATE_NEW_CONSOLE = 0x00000010
    
    # 1. Start Dashboard
    print("Starting Dashboard...")
    subprocess.Popen([PY_EXE, DASH_SCRIPT], creationflags=CREATE_NEW_CONSOLE)
    
    time.sleep(2) # Wait for Flask
    
    # 2. Start Master (Master will NOT launch dashboard anymore)
    print("Starting Master...")
    subprocess.Popen([PY_EXE, MASTER_SCRIPT], creationflags=CREATE_NEW_CONSOLE)

else:
    # Linux/Mac fallback
    subprocess.Popen([PY_EXE, DASH_SCRIPT])
    time.sleep(2)
    subprocess.Popen([PY_EXE, MASTER_SCRIPT])

print()
print(" System Launched!")
print(" Go to: http://localhost:5000")
print("Press ENTER to exit this launcher (processes will keep running)...")
input()
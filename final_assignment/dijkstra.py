import redis
from config import *

class DijkstraScholtenManager:
    # Ο Dijkstra-Scholten είναι ένας αλγόριθμος για την ανίχνευση τερματισμού σε κατανεμημένα συστήματα. (μαθημα)
    # Σε αυτό το πλαίσιο, χρησιμοποιούμε τον Dijkstra-Scholten για να παρακολουθούμε την πρόοδο των εργασιών που εκτελούν οι workers και να ανιχνεύουμε πότε ολοκληρώθηκε όλη η δουλειά χωρίς να χαθεί τίποτα λόγω αποτυχίας κάποιου worker.
    
    def __init__(self, redis_conn):
        self.r = redis_conn
        
    # reset(): Καθαρίζει τα δεδομένα που σχετίζονται με τον Dijkstra-Scholten στον Redis, δηλαδή τα deficits και το γράφο εργασιών. Επίσης,
    # αρχικοποιεί το deficit του Master σε 0 για να ξεκινήσει η διαδικασία.
    def reset(self):
        self.r.delete(KEY_DS_DEFICIT, KEY_DS_GRAPH)
        self.r.hset(KEY_DS_DEFICIT, "Master", 0)

    # send_work(sender_id, task_id): Κάθε φορά που ένας worker στέλνει ένα URL σε έναν άλλο worker για επεξεργασία, 
    # καλεί αυτή τη μέθοδο για να αυξήσει το deficit του εαυτού του κατά 1.
    def send_work(self, sender_id, task_id): 
        try:
            self.r.hincrby(KEY_DS_DEFICIT, sender_id, 1)
        except: pass
    # ack_work(parent_id): Όταν ένας worker ολοκληρώνει την επεξεργασία ενός URL και στέλνει ένα ACK πίσω στον worker 
    # που του το ανέθεσε,
    # καλεί αυτή τη μέθοδο για να μειώσει το deficit του parent worker κατά 1.
    def ack_work(self, parent_id):
        if parent_id:
            try:
                # hincrby με αρνητικό αριθμό κάνει decrement, δηλαδή μειώνει το deficit του parent worker κατά 1,
                self.r.hincrby(KEY_DS_DEFICIT, parent_id, -1)
            except: pass
    # get_status(): Επιστρέφει την τρέχουσα κατάσταση των deficits για όλους τους workers, καθώς και το συνολικό deficit.
    def get_status(self):
        try:
            # παίρνουμε όλα τα deficits από το Redis, 
            # τα καθαρίζουμε για να έχουμε μόνο ακέραιους αριθμούς, και υπολογίζουμε το συνολικό deficit. 
            # Επίσης, ελέγχουμε αν το σύστημα έχει τερματιστεί, δηλαδή αν το συνολικό deficit είναι 0 και έχουμε 
            # παρακολουθήσει τουλάχιστον έναν worker (για να αποφύγουμε το σενάριο όπου δεν έχουμε ξεκινήσει κανένα task).
            deficits = self.r.hgetall(KEY_DS_DEFICIT)
            clean_deficits = {}
            if deficits:
                for k, v in deficits.items():
                    try:
                        clean_deficits[k] = int(v)
                    except: pass 
            
            total_deficit = sum(clean_deficits.values())
            # Terminated if Deficit is 0 AND we have tracked at least one node
            is_terminated = (total_deficit == 0) and (len(clean_deficits) > 0)
            
            return {
                'deficits': clean_deficits,
                'total_deficit': total_deficit,
                'terminated': is_terminated
            }
        except:
            return {'deficits': {}, 'total_deficit': -1, 'terminated': False}
    # transfer_deficit(from_id, to_id): Μεταφέρει ατομικά το deficit από έναν νεκρό worker σε έναν επιλεγμένο 
    # ζωντανό worker χρησιμοποιώντας Lua.
    # χρησιμοποιούμε ένα Lua script για να διασφαλίσουμε ότι η μεταφορά του deficit γίνεται ατομικά, 
    # δηλαδή ότι δεν θα έχουμε race conditions όπου ένα ACK φτάνει ενώ διαβάζουμε/διαγράφουμε τα στατιστικά του νεκρού worker.
    def transfer_deficit(self, from_id, to_id):
        """
        Atomically transfers the deficit from one node to another using Lua.
        This prevents race conditions where an ACK arrives while we are reading/deleting the dead worker's stats.
        """
        lua_script = """
        local val = redis.call('HGET', KEYS[1], ARGV[1])
        if val then
            redis.call('HINCRBY', KEYS[1], ARGV[2], val)
            redis.call('HDEL', KEYS[1], ARGV[1])
            return val
        else
            return 0
        end
        """
        try:
            self.r.eval(lua_script, 1, KEY_DS_DEFICIT, from_id, to_id)
        except Exception as e:
            print(f"Transfer Error: {e}")
            
'''
Aλγοριθμος dijisktra-scholten , 
καθε φορα που ενας κομβος στελνει ενα url σε εναν αλλο κομβο (worker b), χρεωνει τον ευατο του + 1 
και οταν ο worker b τελειωνει το url και στειλει ack πισω στον worker a, 
ο worker a κανει -1 στο deficit του. Αν ο worker a πεθανει ενω εχει θετικο deficit, 
τοτε μεταφερουμε το deficit του σε εναν επιλεγμενο ζωντανο worker
για να μην χαθει η δουλεια που εκανε ο νεκρος worker. 
Ο master μπορει να παρακολουθει τα deficits για να ξερει ποιοι workers ειναι ενεργοι και ποιοι εχουν δουλεια, 
και μπορει να αποφασισει να τερματισει τη διαδικασια οταν ολα τα deficits γινουν 0,
που σημαινει οτι ολοκληρωθηκε η δουλεια χωρις να χαθει τιποτα απο κανεναν νεκρο worker.
'''     



"""
send_work(sender_id, task_id): Κάθε φορά που ένας worker στέλνει ένα URL σε έναν άλλο worker για επεξεργασία,
καλεί αυτή τη μέθοδο για να αυξήσει το deficit του εαυτού του κατά 1. 
Αυτό σημαίνει ότι έχει αναλάβει μια νέα εργασία που πρέπει να ολοκληρωθεί.
"""

""" 
ack_work(parent_id): Όταν ένας worker ολοκληρώνει την επεξεργασία ενός URL και στέλνει ένα ACK πίσω στον worker που του το ανέθεσε,
καλεί αυτή τη μέθοδο για να μειώσει το deficit του parent worker κατά 1.
Αυτό σημαίνει ότι έχει ολοκληρώσει μια εργασία που του είχε ανατεθεί και επιστρέφει την ευθύνη πίσω στον parent worker.
"""      


""" 
Το συστημα τερματιζει
1 ουρα ειναι αδεια
2 ολοι οι worker ειναι idle
ΚΑΙ ΤΟ ΣΥΝΟΛΙΚΟ DEFICIT ΕΙΝΑΙ 0
"""

""" 
επισης το deficity ειναι ενας μετρητης μεσα σε ενα redis has (KEY_DS_DEFICIT) καθε worker ( και ο μαστερ) εχει το δικο του πεδιο
εκει μεσα ..

"""

# Δεν ειναι και πολυ καλο , .....
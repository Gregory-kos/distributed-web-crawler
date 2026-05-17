import redis
from flask import Flask, render_template, jsonify, request
import json
import time
import os
import sys
import subprocess
import threading
import random
from collections import deque
from config import *
from database import DatabaseManager

app = Flask(__name__)
db = DatabaseManager()
# Redis connection for dashboard (separate from master/workers to avoid blocking)
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
# last_count και last_ts χρησιμοποιούνται για τον υπολογισμό της ταχύτητας (speed) του crawler,
# δηλαδή πόσες σελίδες επεξεργάζεται ανά δευτερόλεπτο.
last_count = 0
last_ts = time.time()
chaos_mode_active = False
# για να δοκιμάσουμε την ανθεκτικότητα του συστήματος, έχουμε έναν "Chaos Monkey" thread που τρέχει παράλληλα με το dashboard 
# και κάθε 8-15 δευτερόλεπτα ελέγχει αν το chaos mode είναι ενεργό. Αν ναι, παίρνει τη λίστα των ενεργών workers από το Redis, 
# και αν έχουμε περισσότερους από 1 worker, επιλέγει τυχαία έναν για να "σκοτώσει" βάζοντας ένα kill flag στο Redis 
# και διαγράφοντας την κατάσταση του worker. Αυτό προσομοιώνει μια αποτυχία worker και μας επιτρέπει να δοκιμάσουμε πώς το σύστημα
# χειρίζεται τέτοιες καταστάσεις. Επίσης, καταγράφουμε αυτήν την ενέργεια στα logs του Redis για να είναι ορατή στο dashboard. 
# Αν το chaos mode δεν είναι ενεργό, απλά κοιμόμαστε για 1 δευτερόλεπτο πριν ελέγξουμε ξανά.
# also known as zombie apocalypse  
def chaos_monkey():
    """Randomly kills workers to test system resilience"""
    while True: #  this loop runs no stop in a separate thread, and every 8 to 15 seconds 
        # we check if chaos mode is active, if yes we get the list of active workers from redis, and if we have more than 1 worker we randomly select one to kill by setting a kill flag in redis and deleting its status. This simulates a worker failure and allows us to test how the system handles it. We also log this action in the logs list in redis for visibility on the dashboard. If chaos mode is not active, we simply sleep for 1 second before checking again.
        if chaos_mode_active:
            try:
                # Get active workers
                keys = r.keys("crawler:worker_status:*")
                active_workers = []
                for k in keys:
                    try:
                        w_data = json.loads(r.get(k))
                        if w_data.get('id') != 'MASTER':
                            active_workers.append(w_data.get('id'))
                    except: pass
                
                # Only kill if we have healthy fleet (>1 worker)
                if len(active_workers) > 1:
                    victim = random.choice(active_workers)
                    r.setex(f"crawler:kill:{victim}", 10, "1")
                    r.delete(f"crawler:worker_status:{victim}")
                    
                    ts = time.strftime("[%H:%M:%S]")
                    r.lpush(KEY_LOGS, f"{ts}  CHAOS MONKEY: Killed {victim}!")
            except Exception as e:
                print(f"Chaos Error: {e}")
            
            # Wait random time 8-15s
            time.sleep(random.randint(8, 15))
        else:
            time.sleep(1)

# Start Chaos Thread
# αυτο το thread ξεκινάει παράλληλα με το dashboard και τρέχει τη συνάρτηση chaos_monkey, 
# η οποία είναι υπεύθυνη για την τυχαία "δολοφονία" workers όταν το chaos mode είναι ενεργό. 
# Το daemon=True σημαίνει ότι αυτό το thread θα τερματιστεί αυτόματα όταν τερματιστεί το κύριο πρόγραμμα 
# (π.χ., όταν κλείσουμε το dashboard), οπότε δεν χρειάζεται να ανησυχούμε για το να αφήσουμε "ορφανά" threads να τρέχουν.
threading.Thread(target=chaos_monkey, daemon=True).start()

history = {'labels': deque(maxlen=60), 'queue': deque(maxlen=60), 'speed': deque(maxlen=60)}
for i in range(60):
    history['labels'].append(time.strftime('%H:%M:%S', time.gmtime(time.time() - (60-i))))
    history['queue'].append(0)
    history['speed'].append(0)

@app.route('/')
def index(): return render_template('dashboard.html')
# API ENDPOINTS
@app.route('/api/depth', methods=['POST'])
def set_depth():
    try:
        d = int(request.json.get('depth', 3))
        r.set("crawler:config:depth", d)
        return jsonify({'status': 'ok'})
    except: return jsonify({'status': 'error'})
# αυτό το endpoint επιτρέπει στο dashboard να ενημερώνει την τρέχουσα ταχύτητα (speed) του crawler,
# η οποία υπολογίζεται με βάση τον αριθμό των σελίδων που έχουν επεξεργαστεί σε σχέση με τον χρόνο που έχει περάσει από την
# τελευταία ενημέρωση. Το dashboard στέλνει ένα POST request με το νέο limit, και το backend το αποθηκεύει στο Redis 
# για να το διαβάζουν οι workers και να προσαρμόζουν την ταχύτητά τους ανάλογα.
@app.route('/api/speed', methods=['POST'])
def set_speed():
    try:
        limit = int(request.json.get('limit', 10))
        r.set("crawler:config:rate_limit", limit)
        return jsonify({'status': 'ok'})
    except: return jsonify({'status': 'error'})
# αυτό το endpoint επιτρέπει στο dashboard να ενημερώνει το μέγιστο βάθος (depth) που θα εξερευνά ο crawler,
# το οποίο αποθηκεύεται στο Redis και διαβάζεται από τους workers για να περιορίσουν το πόσο βαθιά θα ακολουθούν τους συνδέσμους 
# σε κάθε σελιδα
@app.route('/api/data')
def get_data():
    global last_count, last_ts
    now = time.time()
    
    data = {
        'stats': {'total': 0, 'queue': 0, 'speed': 0, 'db_size': 0, 'limit': 10, 'tokens': 0.0},
        'history': {'speed': list(history['speed']), 'labels': list(history['labels'])},
        'depth': 3,
        'paused': r.get("crawler:state:paused") == "1",
        'chaos': chaos_mode_active,
        'workers': [],
        'logs': [],
        'dijkstra': 0,
        'dijkstra_map': {},
        'locks': {'writer': None, 'readers': []},
        'domains': {},
        'keywords': []
    }

    try:
        # ATOMIC SNAPSHOT
        pipe = r.pipeline()
        pipe.llen(KEY_RESULTS)
        pipe.llen(KEY_QUEUE)
        pipe.get("crawler:config:depth")
        pipe.lrange(KEY_LOGS, 0, 19)
        pipe.hgetall(KEY_DS_DEFICIT)
        pipe.get("rw:writer")
        pipe.smembers("rw:active_readers")
        pipe.get("crawler:config:rate_limit")
        pipe.get("crawler:semaphore:tokens")
        res = pipe.execute()
        
        total = res[0] or 0
        queue = res[1] or 0
        depth = int(res[2] or 3)
        logs = res[3]
        dm = res[4]
        writer = res[5]
        readers = list(res[6] or [])
        limit = int(res[7] or 10)
        
        # UI CONSISTENCY FIX: 
        # If a writer is active, assume NO readers are active (even if Redis snapshot caught a tail end).
        # This prevents the UI from showing "Reading + Writing" simultaneously due to lag.
        if writer:
            readers = []

        # FIX: Keep tokens as float for smooth visualization
        try: tokens = float(res[8]) if res[8] else 0.0
        except: tokens = 0.0
        
        speed = 0
        if now - last_ts >= 1.0:
            speed = int((total - last_count) / (now - last_ts))
            last_count = total
            last_ts = now
            history['speed'].append(max(0, speed))
        
        data['stats'] = {
            'total': total, 'queue': queue, 'speed': history['speed'][-1], 
            'db_size': db.get_stats().get('db_size', 0), 
            'limit': limit,
            'tokens': tokens
        }
        data['depth'] = depth
        data['logs'] = logs
        data['dijkstra'] = sum(int(v) for v in dm.values()) if dm else 0
        data['dijkstra_map'] = {k: int(v) for k, v in dm.items()} if dm else {}
        data['locks'] = {'writer': writer, 'readers': readers}

        # Workers
        keys = r.keys("crawler:worker_status:*")
        for k in keys:
            try:
                w = json.loads(r.get(k))
                w['heartbeat_ago'] = round(now - w.get('last_seen', now), 1)
                
                # Trust the worker's reported status
                w['status'] = w.get('status', 'IDLE')

                # VISUAL SANITIZATION:
                # If a Writer is active, NO ONE can be visually "Reading".
                # If we see "Reading" while Writer is active, it's stale data. Show "Waiting" instead.
                if writer and w['status'] in ['READING_DB', 'WAITING_READ_LOCK']:
                    w['status'] = 'WAITING_RW_LOCK'
                
                if w['heartbeat_ago'] < 20: 
                    data['workers'].append(w)
                else:
                    # Cleanup
                    r.delete(k)
            except: pass
        data['workers'].sort(key=lambda x: (0 if x.get('id') == 'MASTER' else 1, x.get('id', '')))

        # Domains & Keywords
        raw_res = r.lrange(KEY_RESULTS, 0, 499)
        d_counts = {}
        for item in raw_res:
            try:
                obj = json.loads(item)
                d = obj.get('domain')
                if d: d_counts[d] = d_counts.get(d, 0) + 1
            except: pass
        data['domains'] = dict(sorted(d_counts.items(), key=lambda x: x[1], reverse=True)[:10])

        k_list = r.zrevrange(KEY_KEYWORDS, 0, 14, withscores=True)
        data['keywords'] = [{'word': k, 'count': int(s)} for k, s in k_list]

    except Exception as e: print(f"API Error: {e}")
    return jsonify(data)

@app.route('/api/search')
def search():
    query = request.args.get('q', '').strip()
    if not query: return jsonify([])
    return jsonify(db.search_pages(query))

@app.route('/api/control/pause', methods=['POST'])
def toggle_pause():
    current = r.get("crawler:state:paused")
    if current == "1":
        r.delete("crawler:state:paused")
        return jsonify({'status': 'running'})
    else:
        r.set("crawler:state:paused", "1")
        return jsonify({'status': 'paused'})

@app.route('/api/control/chaos', methods=['POST'])
def toggle_chaos():
    global chaos_mode_active
    chaos_mode_active = not chaos_mode_active
    status = 'on' if chaos_mode_active else 'off'
    ts = time.strftime("[%H:%M:%S]")
    r.lpush(KEY_LOGS, f"{ts} UI: Chaos Mode turned {status.upper()}")
    return jsonify({'status': status})
# αυτο το endpoint επιτρεπει στο  dashboard να "δολοφονεί" έναν worker με βάση το ID του, 
# βάζοντας ένα kill flag στο Redis και διαγράφοντας την κατάσταση του worker.
# Αυτό χρησιμοποιείται για να δοκιμάσουμε την ανθεκτικότητα του συστήματος σε αποτυχίες worker μέσω του dashboard.
@app.route('/api/control/kill', methods=['POST'])
def kill_worker():
    wid = request.json.get('id')
    if wid:
        r.setex(f"crawler:kill:{wid}", 10, "1")
        r.delete(f"crawler:worker_status:{wid}")
    return jsonify({'status': 'ok'})

@app.route('/api/db/stats')
def db_stats():
    try:
        stats = db.get_stats()
        recent = db.get_recent(limit=10)
        efficiency = db.get_worker_efficiency()
        return jsonify({
            'total_pages': stats.get('total', 0), 
            'file_size_kb': stats.get('db_size', 0), 
            'avg_time': stats.get('avg_time', 0), 
            'top_worker': stats.get('top_worker', '-'), 
            'recent_entries': [dict(row) for row in recent],
            'efficiency': efficiency
        })
    except: return jsonify({'recent_entries': [], 'efficiency': []})
# αυτό το endpoint επιτρέπει στο dashboard να λαμβάνει στατιστικά στοιχεία από τη βάση δεδομένων, 
# όπως τον συνολικό αριθμό σελίδων που έχουν επεξεργαστεί, το μέγεθος του αρχείου της βάσης δεδομένων,
# τον μέσο χρόνο επεξεργασίας ανά σελίδα, τον πιο αποδοτικό worker, καθώς και τις πιο πρόσφατες καταχωρήσεις στη βάση δεδομένων. Αυτά τα δεδομένα χρησιμοποιούνται για να ενημερώσουν τα στατιστικά στοιχεία που εμφανίζονται στο dashboard.
@app.route('/api/data/recent')
def api_recent():
    res = []
    try:
        raw = r.lrange(KEY_RESULTS, 0, 49)
        for item in raw:
            try:
                o = json.loads(item)
                res.append({'url': o.get('url', '?'), 'worker': o.get('worker', '?'), 'depth': o.get('depth', 0)})
            except: pass
    except: pass
    return jsonify(res)

@app.route('/api/graph')
def api_graph():
    nodes = []; edges = []; seen = set()
    try:
        raw = r.lrange("crawler:graph_edges", 0, 99)
        for item in raw:
            try:
                e = json.loads(item)
                u, v = e.get('from'), e.get('to')
                if u and v:
                    if u not in seen: {nodes.append({'id': u, 'label': '', 'color': '#0d6efd', 'size': 10}), seen.add(u)}
                    if v not in seen: {nodes.append({'id': v, 'label': '', 'color': '#198754', 'size': 5}), seen.add(v)}
                    edge_id = f"{u}_{v}"
                    edges.append({'id': edge_id, 'from': u, 'to': v})
            except: pass
    except: pass
    return jsonify({'nodes': nodes, 'edges': edges})

@app.route('/api/spawn', methods=['POST'])
def spawn():
    try:
        cwd = os.path.dirname(os.path.abspath(__file__))
        cmd = [sys.executable, "worker_node.py"]
        
        if os.name == 'nt':
            # Windows: Try to open in new console for visibility
            # 0x00000010 is CREATE_NEW_CONSOLE
            subprocess.Popen(cmd, cwd=cwd, creationflags=0x00000010)
        else:
            # Linux/Docker: Run in background
            subprocess.Popen(cmd, cwd=cwd)
            
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'status': 'error', 'msg': str(e)})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)
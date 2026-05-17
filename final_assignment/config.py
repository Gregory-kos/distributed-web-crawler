import os

# --- REDIS CONFIG ---
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = 6379
REDIS_DB = 0

# --- CRAWLER KEYS ---
KEY_QUEUE = 'crawler:queue'
KEY_VISITED = 'crawler:visited'
KEY_RESULTS = 'crawler:results'
KEY_LOGS = 'crawler:logs'
KEY_STATUS = 'crawler:status'
KEY_KEYWORDS = 'crawler:keywords'

# --- DIJKSTRA-SCHOLTEN KEYS ---
KEY_DS_DEFICIT = 'ds:deficit'
KEY_DS_GRAPH = 'ds:graph'
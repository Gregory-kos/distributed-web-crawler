import random

class MiddlewareManager:
    """
    Διαχειρίζεται User-Agents και Proxies.
    """
    def __init__(self):
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) Firefox/90.0',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15'
        ]
        # Fake Proxies list for visualization purposes , στην πραγματικότητα θα πρέπει να έχουμε μια λίστα με πραγματικούς proxies που μπορούμε να χρησιμοποιήσουμε για να κάνουμε τα requests μας, για να μην μπλοκαριστούμε από τους servers που επισκεπτόμαστε. Εδώ απλά βάζουμε μερικά παραδείγματα για να δείξουμε τη λειτουργία.
        self.proxies = [
            'http://192.168.1.10:8080', 
            'http://10.0.0.5:3128', 
            'http://45.76.12.9:80', 
            'http://203.0.113.45:8888',
            'http://proxy-us.vpn.com:1080',
            'http://proxy-eu.vpn.com:1080'
        ]
        
    def get_headers(self):
        return {'User-Agent': random.choice(self.user_agents)}

    def get_proxy(self):
        # 20% chance to go Direct (None)
        if random.random() > 0.8:
            return None
        return random.choice(self.proxies)
    
    # fake !!!!!!!!!!!
    
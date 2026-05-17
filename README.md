# 🕷️ Distributed Web Crawler with Fault Tolerance & Termination Detection

A high-performance, distributed web crawling system built with **Python**, **Redis**, and **SQLite**. This project implements advanced distributed systems concepts, including the **Dijkstra-Scholten algorithm** for termination detection, **Reliable Queue Patterns**, and **Custom Distributed Concurrency Primitives**.

---

## 🚀 Key Features

- **Distributed Architecture**: Scalable Master-Worker model using Redis as a high-speed message broker and state store.
- **Termination Detection**: Accurate completion tracking using the **Dijkstra-Scholten algorithm**, ensuring the system stops only when all tasks are truly finished.
- **Fault Tolerance**: 
    - **Heartbeat Monitoring**: Master node tracks worker health in real-time.
    - **Reliable Recovery**: Automatic task re-queuing using the **Reliable Queue Pattern** (RPOPLPUSH) if a worker crashes.
    - **Atomic Deficit Transfer**: Lua-scripted deficit transfer to maintain Dijkstra-Scholten integrity during node failures.
- **Distributed Concurrency Control**:
    - **Distributed RW-Lock**: Multi-reader, single-writer lock implemented via Redis Lua scripts to protect the shared SQLite database.
    - **Distributed Semaphore (Rate Limiter)**: Token-bucket based rate limiting to respect target server constraints.
- **Interactive Dashboard**: Real-time Flask-based web interface for monitoring crawl progress, worker stats, and logs.
- **Containerized Deployment**: Ready-to-run with Docker and Docker Compose.

---

## 🏗️ System Architecture

### 1. Master Node (The Brain)
- Coordinates the entire fleet.
- Handles initial seeding.
- Monitors worker heartbeats and performs fault recovery.
- Runs the web dashboard.
- Tracks global termination state.

### 2. Worker Nodes (The Muscle)
- Autonomous units competing for tasks.
- Perform safe pops from the queue (ensuring no task loss).
- Respect rate limits and synchronize access to shared data using distributed locks.
- Report real-time metrics (CPU, current URL, status).

### 3. Distributed Components
- **Redis**: Acts as the backbone for the task queue, locking mechanisms, heartbeats, and coordination metadata.
- **SQLite**: Persistent storage for crawled page results (Title, Keywords, URL).

---

## 🛠️ Technical Deep Dive

### Dijkstra-Scholten Algorithm
In an asynchronous distributed system, knowing when "everyone is done" is hard. We implement a **Deficit-based** tracking system:
- **Send Work**: Increment sender's deficit.
- **Ack Work**: Decrement parent's deficit when a task and its descendants are finished.
- **Termination**: Total Deficit == 0 and Queue is empty.

### Reliable Queue Pattern
Standard `BRPOP` is unsafe; if a worker crashes after popping, the task is lost. We use `BRPOPLPUSH` to atomically move tasks to a "processing" list. If the worker's heartbeat fails, the Master retrieves tasks from this list and re-queues them.

### Lua-Powered Atomic Operations
To prevent race conditions, complex operations like **RW-Lock acquisition** and **Dijkstra Deficit Transfer** are implemented as **Redis Lua Scripts**, ensuring they execute atomically on the server side.

---

## 🚦 Getting Started

### Prerequisites
- Python 3.9+
- Redis Server (local or via Docker)

### Option A: Running with Docker (Recommended)
The easiest way to see the system in action:
```bash
docker-compose up --build
```
This will start 1 Redis instance, 1 Master node, and 5 Worker nodes.

### Option B: Local Manual Execution
1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
2. **Start Redis** (Ensure a redis-server is running on localhost:6379).
3. **Launch the System**:
   ```bash
   python final_assignment/main.py
   ```
   *This launcher will automatically open the dashboard in your browser.*

---

## 📊 Monitoring
Once running, visit the dashboard at:
**[http://localhost:5000](http://localhost:5000)**

From here you can:
- See active workers and their CPU/Memory usage.
- View real-time logs of the crawling process.
- Monitor the queue size and total deficit.
- Inspect crawled data.

---

## 📂 Project Structure
- `final_assignment/`: Main source code.
    - `master_node.py`: Orchestrator logic.
    - `worker_node.py`: Crawling and worker logic.
    - `dijkstra.py`: Termination detection manager.
    - `custom_queue.py`: Reliable Redis queue implementation.
    - `rw_lock.py` & `semaphore.py`: Distributed concurrency primitives.
- `documentation/`: Detailed markdown docs for every module.
- `templates2/`: Flask UI templates.
- `Dockerfile` & `docker-compose.yml`: Containerization setup.

---

## 🎓 Academic Context
This project was developed as an **optional assignment** for the **Concurrency Programming** course (Προγραμματισμός Ταυτοχρονισμού).

Το έργο αυτό υλοποιήθηκε στα πλαίσια του μαθήματος **Προγραμματισμός Ταυτοχρονισμού** ως **προαιρετική απαλλακτική εργασία**.

---

## 📝 License
This project was developed for educational purposes as part of a distributed systems and concurrency study.

"""
Nancy Kiden Luka
SCS3/148364/2024
Bully algorithm

Messages shown in the log:
    ELECTION  (the board's "e")   sent to every node with a higher ID
    OK                            reply from a live higher node
    I WON                         winner announces itself to everybody
    PING / PONG                   heartbeat: "is the coordinator still alive?"

  * a TIMEOUT: if no OK arrives, every higher node is down, so I win;
  * a HEARTBEAT: each node pings the coordinator now and then; no PONG
    means the coordinator crashed, so the node starts an election.
"""
import queue
import random
import threading
import time

TIMEOUT = 0.5               # wait for OK / PONG before assuming "down"
ELECTION_WAIT = 2.0         # after an OK, wait this long for the winner's I WON
HEARTBEAT_INTERVAL = 2.0    # how often a node pings the coordinator

ELECTION, OK, I_WON, PING, PONG = "ELECTION", "OK", "I WON", "PING", "PONG"

_print_lock = threading.Lock()
_t0 = time.time()

def log(msg):
    with _print_lock:
        print(f"[{time.time() - _t0:5.1f}s] {msg}", flush=True)

class Network:
    """Delivers messages. Crashed nodes neither send nor receive."""

    def __init__(self):
        self.nodes = {}

    def send(self, frm, to, kind):
        if not self.nodes[frm].alive:
            return
        target = self.nodes[to]
        if target.alive:
            log(f"    {frm} --{kind}--> {to}")
            target.inbox.put((kind, frm))
        else:
            log(f"    {frm} --{kind}--> {to}    ({to} is DOWN, no reply)")

class Node(threading.Thread):
    def __init__(self, node_id, network):
        super().__init__(daemon=True)
        self.id = node_id
        self.net = network
        self.alive = True
        self.inbox = queue.Queue()
        self.coordinator = None
        self.electing = False
        self.lock = threading.Lock()
        self.got_ok = threading.Event()
        self.got_coordinator = threading.Event()
        self.got_pong = threading.Event()
        self.stop_flag = threading.Event()
        threading.Thread(target=self.heartbeat, daemon=True).start()

    def send(self, to, kind):
        self.net.send(self.id, to, kind)

    # Main loop  
    def run(self):
        while True:
            msg = self.inbox.get()                
            if msg is None:
                return
            st, sender = msg
            if not self.alive:
                continue

            if st == ELECTION:                     # if (St == "e")
                self.send(sender, OK)              #     respond("OK")
                threading.Thread(                  #     start_election()
                    target=self.start_election, daemon=True).start()
            elif st == OK:
                self.got_ok.set()
            elif st == I_WON:
                self.coordinator = sender
                self.got_coordinator.set()
            elif st == PING:                       # heartbeat from another node
                self.send(sender, PONG)
            elif st == PONG:
                self.got_pong.set()

    # start_election()
    def start_election(self):
        with self.lock:
            if self.electing or not self.alive:
                return
            self.electing = True
        try:
            while self.alive:
                self.got_ok.clear()
                self.got_coordinator.clear()
                higher = [i for i in self.net.nodes if i > self.id]

                if not higher:
                    self.declare_victory()
                    return

                log(f"Node {self.id} starts an election")
                for h in higher:                   # else send("e")
                    self.send(h, ELECTION)

                if not self.got_ok.wait(TIMEOUT):
                    # No higher node answered: they are all down, so I win.
                    self.declare_victory()
                    return

        
                if self.got_coordinator.wait(ELECTION_WAIT):
                    return
                log(f"Node {self.id}: nobody announced I WON, retrying")
        finally:
            self.electing = False

    def declare_victory(self):
        log(f"Node {self.id} wins the election and becomes COORDINATOR")
        self.coordinator = self.id
        for other in self.net.nodes:
            if other < self.id:              # I WON goes only to lower nodes
                self.send(other, I_WON)

    # Heartbeat: is the coordinator still alive?
    def heartbeat(self):
        self.stop_flag.wait(random.uniform(0, HEARTBEAT_INTERVAL))
        while not self.stop_flag.wait(HEARTBEAT_INTERVAL):
            if not self.alive or self.electing:
                continue
            c = self.coordinator
            if c is None or c == self.id:
                continue
            self.got_pong.clear()
            self.send(c, PING)
            if not self.got_pong.wait(TIMEOUT):
                log(f"Node {self.id}: coordinator {c} did not answer PING "
                    f"-> assumes it crashed")
                self.coordinator = None
                self.start_election()
class Cluster:
    def __init__(self, n):
        self.net = Network()
        for i in range(1, n + 1):
            self.net.nodes[i] = Node(i, self.net)
        for node in self.net.nodes.values():
            node.start()

    def node(self, i):
        return self.net.nodes[i]

    def crash(self, i):
        log(f"*** Node {i} CRASHES ***")
        n = self.node(i)
        n.alive = False
        n.coordinator = None

    def recover(self, i):
        log(f"*** Node {i} RECOVERS ***")
        n = self.node(i)
        n.alive = True
        n.coordinator = None
        n.start_election()      # a recovered node always starts an election

    def settle(self, expected, timeout=20):
        """Wait until every live node agrees on `expected` as coordinator."""
        live = [n for n in self.net.nodes.values() if n.alive]
        deadline = time.time() + timeout
        while time.time() < deadline:
            if all(n.coordinator == expected and not n.electing for n in live):
                break
            time.sleep(0.1)
        seen = {n.id: n.coordinator for n in live}
        assert all(c == expected for c in seen.values()), f"disagreement: {seen}"
        log(f"OK: all live nodes agree - coordinator is Node {expected}  {seen}")

    def shutdown(self):
        for n in self.net.nodes.values():
            n.stop_flag.set()
            n.inbox.put(None)

def phase(title):
    with _print_lock:
        print(f"\n{title}", flush=True)

if __name__ == "__main__":
    cluster = Cluster(6)

    phase("1. Start-up: Node 4 sees there is no coordinator and starts an election")
    cluster.node(4).start_election()
    cluster.settle(expected=6)

    phase("2. Normal operation: nodes PING the coordinator (Node 6)")
    time.sleep(3 * HEARTBEAT_INTERVAL / 2 + 1)

    phase("3. Coordinator 6 crashes -> a PING goes unanswered -> new election")
    cluster.crash(6)
    cluster.settle(expected=5)

    phase("4. New coordinator 5 crashes too")
    cluster.crash(5)
    cluster.settle(expected=4)

    phase("5. Node 6 recovers and bullies its way back to coordinator")
    cluster.recover(6)
    cluster.settle(expected=6)

    time.sleep(1)
    cluster.shutdown() 

import json, random, socket, sys, threading, time

PORTS = {"P": 9001, "A": 9002, "B": 9003, "C": 9004, "D": 9005}
NEIGHBOURS = {"P": ["A", "B"], "A": ["P", "C"], "B": ["P", "D"], "C": ["A", "D"], "D": ["B", "C"]}
ROUND = 3.0  # seconds per round

name = sys.argv[1]
lock = threading.Lock()
state = {"have": False, "text": None, "origin": None, "got_tick": None, "known": set()}
start = time.time()


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def tick():
    return int(time.time() // ROUND)


def handle(conn):  # a neighbour pushed something to us
    with conn:
        msg = json.loads(conn.recv(4096).decode())
        with lock:
            is_new = not state["have"]
            state["known"] |= set(msg["informed"])
            if is_new:
                state.update(have=True, text=msg["text"], origin=msg["origin"], got_tick=tick())
                state["known"].add(name)
        conn.sendall(b"NEW" if is_new else b"HAVE")
    if is_new:
        log(f"*** {name} RECEIVED '{msg['text']}' from {msg['from']} (round {tick() - msg['origin'] + 1})")


def push(peer):
    with lock:
        payload = {"from": name, "text": state["text"], "origin": state["origin"],
                   "informed": sorted(state["known"])}
        rnd = tick() - state["origin"] + 1
    try:
        with socket.create_connection(("127.0.0.1", PORTS[peer]), timeout=2) as s:
            s.sendall(json.dumps(payload).encode())
            reply = s.recv(16).decode()
        with lock:
            state["known"].add(peer)  # either way, peer has the update now
        if reply == "NEW":
            log(f"{name} GOSSIPED WITH {peer} (round {rnd})")
        else:
            log(f"{name} pushed to {peer} but it already had the update (round {rnd})")
    except OSError:
        log(f"{name} could not reach {peer}")


def rounds():
    while True:
        time.sleep(ROUND - time.time() % ROUND)  # wait for the next round boundary
        t = tick()
        with lock:
            if name == "P" and not state["have"] and time.time() - start >= 4:
                state.update(have=True, text="Hello from P!", origin=t, got_tick=t - 1)
                state["known"].add("P")
                log("*** P ORIGINATED update 'Hello from P!' (round 1)")
                
            # act only in rounds AFTER the one in which we received the update
            ready = state["have"] and state["got_tick"] < t
            candidates = [n for n in NEIGHBOURS[name] if n not in state["known"]]
        if ready and candidates:
            push(random.choice(candidates))


srv = socket.socket()
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", PORTS[name]))
srv.listen()
log(f"{name} listening on port {PORTS[name]}, neighbours = {NEIGHBOURS[name]}")
threading.Thread(target=rounds, daemon=True).start()

while True:
    conn, _ = srv.accept()
    threading.Thread(target=handle, args=(conn,)).start()
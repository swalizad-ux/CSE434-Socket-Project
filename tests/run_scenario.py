"""Integration test: runs a real manager and six peer processes on localhost against a
synthetic NOAA-format details-1996.csv and checks every command.
    cd tests && python3 gen_csv.py && python3 run_scenario.py"""
import os, re, subprocess, sys, threading, time, queue

CODE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, CODE)
from protocol import first_prime_greater_than, hash_position, hash_node

procs, logs = {}, {}
fails = []


def check(cond, msg):
    print(("  PASS  " if cond else "  FAIL  ") + msg)
    if not cond:
        fails.append(msg)


def start(name, args):
    p = subprocess.Popen([sys.executable, "-u", os.path.join(CODE, args[0])] + args[1:],
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT, text=True, cwd=HERE)
    procs[name] = p
    logs[name] = []

    def reader():
        for line in p.stdout:
            logs[name].append(line.rstrip("\n"))
    threading.Thread(target=reader, daemon=True).start()


def send(name, cmd):
    procs[name].stdin.write(cmd + "\n")
    procs[name].stdin.flush()


def wait_for(name, pattern, timeout=60, start=0):
    end = time.time() + timeout
    while time.time() < end:
        for i, ln in enumerate(logs[name][start:], start):
            if re.search(pattern, ln):
                return i, ln
        time.sleep(0.05)
    print(f"--- TIMEOUT waiting for /{pattern}/ in {name}; last lines:")
    print("\n".join(logs[name][-15:]))
    return None, None


def run(name, cmd, pattern, timeout=60):
    start_i = len(logs[name])
    send(name, cmd)
    return wait_for(name, pattern, timeout, start_i)


def table_counts(names):
    out = {}
    for nm in names:
        i0 = len(logs[nm])
        send(nm, "show-table")
        _, ln = wait_for(nm, r"local table holds", 10, i0)
        m = re.search(r"id=(\w+) ring size=(\w+): local table holds (\d+)", ln)
        out[nm] = (m.group(1), int(m.group(3)))
    return out


# expected distribution -------------------------------------------------
rows = [l for l in open(os.path.join(HERE, "details-1996.csv")).read().splitlines()[1:] if l.strip()]
L = len(rows)
S = first_prime_greater_than(2 * L)
eids = [int(r.split(",")[7]) for r in rows]


def expected(n):
    c = {i: 0 for i in range(n)}
    for e in eids:
        c[hash_node(hash_position(e, S), n)] += 1
    return c


try:
    PEERS = ["alice", "bob", "carol", "dave", "erin", "fran"]
    start("manager", ["manager.py", "1500"])
    time.sleep(0.7)
    for k, nm in enumerate(PEERS):
        start(nm, ["peer.py", "127.0.0.1", "1500"])
    time.sleep(0.5)
    print("== register 6 peers")
    for k, nm in enumerate(PEERS):
        _, ln = run(nm, f"register {nm} 127.0.0.1 {1501 + 2 * k} {1502 + 2 * k}", r"register ->")
        check("SUCCESS" in ln, f"{nm} registered")
    start("dup", ["peer.py", "127.0.0.1", "1500"])
    time.sleep(0.4)
    _, ln = run("dup", "register bob 127.0.0.1 1601 1602", r"register ->")
    check("FAILURE" in ln, "duplicate name rejected by manager")
    send("dup", "quit")
    _, ln = run("bob", "register bob 127.0.0.1 1601 1602", r"already registered")
    check(ln is not None, "re-register on a live peer does not touch its sockets")

    print("== setup-dht n=5 1996 (leader alice)")
    t0 = time.time()
    _, ln = run("alice", "setup-dht 5 1996", r"dht-complete ->", 120)
    check("SUCCESS" in ln, f"setup-dht + dht-complete ok ({time.time() - t0:.1f}s)")
    ring = None
    for ln in logs["alice"]:
        m = re.search(r"Ring order: \[(.*)\]", ln)
        if m:
            ring = [x.strip("' ") for x in m.group(1).split(",")]
    print("   ring:", ring)
    member_set = set(ring)
    outside = [p for p in PEERS if p not in member_set]
    check(len(outside) == 1, f"one peer outside DHT: {outside}")
    exp5 = expected(5)
    c = table_counts(ring)
    got = {int(v[0]): v[1] for v in c.values()}
    check(got == exp5, f"per-node record counts match hash expectation {exp5} vs {got}")
    check(sum(got.values()) == L, f"no records lost: {sum(got.values())} == {L}")

    print("== manager FAILs other commands while waiting? (setup again)")
    _, ln = run(outside[0], "setup-dht 3 1996", r"setup-dht ->")
    check("FAILURE" in ln, "second setup-dht rejected (DHT exists)")

    q = outside[0]
    print(f"== query-dht from {q}")
    for eid in [5536849, 2402920, 5539287, 55770111]:
        i0 = len(logs[q])
        send(q, f"query-dht {eid}")
        idx, ln = wait_for(q, rf"Storm event {eid} (found|not found)", 30, i0)
        if eid == 55770111:
            check(ln and "not found in the DHT" in ln, f"event {eid} -> not found message")
        else:
            check(ln and "found" in ln and "not found" not in ln, f"event {eid} found: {ln}")
            time.sleep(0.3)
            blk = "\n".join(logs[q][i0:])
            check("EVENT_ID" in blk and f": {eid}" in blk, "labelled fields printed")
    # a peer inside the DHT may not query
    _, ln = run(ring[1], "query-dht 5536849", r"query-dht ->")
    check("FAILURE" in ln, "non-Free peer cannot query-dht")

    print("== leave-dht (a non-leader member)")
    leaver = ring[2]
    old_leader = ring[0]
    t0 = time.time()
    _, ln = run(leaver, "leave-dht", r"dht-rebuilt \(new leader", 120)
    check(ln and "SUCCESS" in ln, f"leave completed ({time.time() - t0:.1f}s): {ln}")
    new_ring = [m for m in ring if m != leaver]
    new_ring = new_ring[new_ring.index(ring[3]):] + new_ring[:new_ring.index(ring[3])]
    print("   expected new ring:", new_ring)
    exp4 = expected(4)
    c = table_counts(new_ring)
    got = {int(v[0]): v[1] for v in c.values()}
    check(got == exp4, f"after leave: counts match n=4 expectation {exp4} vs {got}")
    check([k for k, v in c.items() if v[0] == "0"] == [new_ring[0]], f"{new_ring[0]} is new leader id 0")
    check(sum(got.values()) == L, "no records lost after leave")
    check(table_counts([leaver])[leaver][1] == 0, "leaver holds no records")

    print("== query by the peer that left")
    i0 = len(logs[leaver])
    send(leaver, "query-dht 2402920")
    _, ln = wait_for(leaver, r"Storm event 2402920 (found|not found)", 30, i0)
    check(ln and "not found" not in ln, f"leaver can query: {ln}")

    print("== join-dht by the other outside peer")
    joiner = outside[0]
    t0 = time.time()
    _, ln = run(joiner, "join-dht", r"dht-rebuilt \(leader", 120)
    check(ln and "SUCCESS" in ln, f"join completed ({time.time() - t0:.1f}s): {ln}")
    exp5b = expected(5)
    c = table_counts(new_ring + [joiner])
    got = {int(v[0]): v[1] for v in c.values()}
    check(got == exp5b, f"after join: counts match n=5 expectation {exp5b} vs {got}")
    check(sum(got.values()) == L, "no records lost after join")
    i0 = len(logs[leaver])
    send(leaver, "query-dht 5539287")
    _, ln = wait_for(leaver, r"Storm event 5539287 (found|not found)", 30, i0)
    check(ln and "not found" not in ln, f"query after join: {ln}")

    print("== teardown by the leader")
    leader = new_ring[0]
    _, ln = run(leader, "teardown-dht", r"teardown-complete ->", 60)
    check(ln and "SUCCESS" in ln, f"teardown completed: {ln}")
    c = table_counts(new_ring + [joiner])
    check(all(v[1] == 0 for v in c.values()), f"all tables empty: {c}")
    _, ln = run(leaver, "query-dht 5539287", r"query-dht ->")
    check("FAILURE" in ln, "query-dht fails after teardown")

    print("== deregister all")
    for nm in PEERS:
        i0 = len(logs[nm])
        send(nm, "deregister")
        wait_for(nm, r"deregister ->", 10, i0)
        ok = any("deregistered; exiting" in l for l in logs[nm][i0:])
        check(ok, f"{nm} deregistered and exited")
    time.sleep(0.5)
    check(all(procs[nm].poll() is not None for nm in PEERS), "all peer processes exited")
finally:
    for p in procs.values():
        try:
            p.kill()
        except Exception:
            pass
    with open(os.path.join(HERE, "manager.log"), "w") as f:
        f.write("\n".join(logs["manager"]))
    for nm in logs:
        with open(os.path.join(HERE, f"{nm}.log"), "w") as f:
            f.write("\n".join(logs[nm]))

print("\nFAILED:" if fails else "\nALL CHECKS PASSED", *fails, sep="\n  " if fails else "")
sys.exit(1 if fails else 0)

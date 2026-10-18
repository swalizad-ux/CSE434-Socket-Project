"""
peer.py -- DHT Peer (full project)

Usage:
    python3 peer.py <manager-ip> <manager-port>

Commands read from stdin:
    register <peer-name> <my-ip> <m-port> <p-port>
    setup-dht <n> <YYYY> [csv-path]     (this peer becomes the leader)
    query-dht <event-id>
    leave-dht
    join-dht
    teardown-dht                        (leader only)
    deregister                          (Free peers only; the process exits)
    show-table                          (debug helper, not part of the spec)
    quit

Threading model
---------------
  * main thread      reads stdin, performs the manager round-trips (blocking
                     request/response on m_sock) and drives the multi-step
                     P2P procedures that this peer initiates (setup, leave,
                     teardown, query).
  * listener thread  owns p_sock (the p-port).  It handles every incoming
                     P2P message at any time -- set-id, store, sync, teardown,
                     reset-id, find-event, ... -- so the peer keeps taking part
                     in the ring while its main thread is blocked on stdin or
                     on a manager reply.  The listener never blocks on a
                     long operation: when a message starts one (join-request,
                     rebuild-dht) it hands the work to a short-lived worker
                     thread.
  * ctrl_sock        a third, ephemeral socket used only for the synchronous
                     set-id request/ack handshake, so that p_sock is never
                     read by two threads at once (that race made the listener
                     steal the leader's acknowledgement in an early version).

Peer-to-peer messages (all '|'-delimited, see protocol.py)
----------------------------------------------------------
  set-id|id|n|s|yyyy|tuples|header     leader -> peer, direct (ring not built yet)
  store|target-id|pos|record           forwarded around the ring
  sync|origin-id|seq                   barrier token around the ring
  teardown|origin-id|clear             delete local table, forwarded around ring
  reset-id|origin|new-id|new-n|tuples  renumber the ring (leave / join)
  rebuild-dht|yyyy|leaver-tuple        leaver -> new leader
  join-request|joiner-tuple            joiner -> leader
  rebuild-done|SUCCESS/FAILURE         new leader / leader -> leaver / joiner
  find-event|event-id|S-tuple|id-seq   hot-potato query
  find-event-response|...              answer sent straight to the querier S
"""

import csv
import os
import random
import socket
import sys
import threading

from protocol import (
    SUCCESS, FAILURE, send_msg, recv_msg, encode_tuples, decode_tuples, tail,
    first_prime_greater_than, hash_position, hash_node,
)

EVENT_ID_COL = 7        # EVENT_ID is the 8th column of the NOAA details CSV
BATCH_BYTES = 32000     # max un-synchronised store payload in flight
SYNC_TIMEOUT = 5.0      # seconds to wait for a sync token to come back
STEP_TIMEOUT = 60.0     # seconds to wait for a ring-wide step (teardown, ...)
REBUILD_TIMEOUT = 900.0 # seconds to wait for a whole rebuild


class Peer:
    def __init__(self, manager_ip, manager_port):
        self.manager_addr = (manager_ip, manager_port)

        self.name = None
        self.ip = None
        self.m_port = None
        self.p_port = None
        self.m_sock = None
        self.p_sock = None
        self.ctrl_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.ctrl_sock.bind(("0.0.0.0", 0))
        self.ctrl_sock.settimeout(STEP_TIMEOUT)

        # ring state (None while this peer is not part of a DHT)
        self.id = None
        self.n = None
        self.s = None               # hash table size (first prime > 2*l)
        self.yyyy = None
        self.header = ""            # CSV header line, used to label fields
        self.members = None         # [(name, ip, p_port)] in ring order
        self.right_neighbor_addr = None

        # Local hash table: dict pos -> list of raw CSV lines.  A list per
        # position (chaining) because different event ids can share a pos.
        self.table = {}
        self.lock = threading.Lock()

        # events used to wait for something the listener thread observes
        self.ev_teardown = threading.Event()
        self.ev_reset = threading.Event()
        self.ev_rebuild = threading.Event()
        self.rebuild_status = None
        self.ev_query = threading.Event()
        self.query_result = None
        self.sync_event = threading.Event()
        self.sync_seq = 0
        self.sync_wait_seq = -1
        self.sync_lock = threading.Lock()

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def log(self, msg):
        print(f"[{self.name}] {msg}", flush=True)

    def my_tuple(self):
        return (self.name, self.ip, self.p_port)

    def set_ring(self, node_id, n, members):
        self.id, self.n, self.members = node_id, n, members
        nxt = members[(node_id + 1) % n]
        self.right_neighbor_addr = (nxt[1], nxt[2])

    def clear_ring(self):
        self.id = self.n = self.members = self.right_neighbor_addr = None

    def table_add(self, pos, line):
        with self.lock:
            self.table.setdefault(pos, []).append(line)

    def table_clear(self):
        with self.lock:
            self.table = {}

    def table_count(self):
        with self.lock:
            return sum(len(v) for v in self.table.values())

    def ask_manager(self, *fields):
        """Send one message to the manager and wait for its reply (messages
        to the manager always come in request/response pairs)."""
        send_msg(self.m_sock, self.manager_addr, *fields)
        reply, _ = recv_msg(self.m_sock)
        return reply

    def find_dataset(self, yyyy, hint=None):
        here = os.path.dirname(os.path.abspath(__file__))
        for cand in (hint, f"details-{yyyy}.csv",
                     os.path.join(here, f"details-{yyyy}.csv")):
            if cand and os.path.isfile(cand):
                return cand
        raise FileNotFoundError(f"cannot find details-{yyyy}.csv in the current "
                                f"directory (needed by any peer that may lead the DHT)")

    def load_dataset(self, path):
        """Return (header, records, s).  l = number of data lines and
        s = first prime strictly greater than 2*l (project description 1.2.1)."""
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = [ln.rstrip("\r\n") for ln in f]
        header = lines[0]
        records = [ln for ln in lines[1:] if ln.strip()]
        l = len(records)
        s = first_prime_greater_than(2 * l)
        self.log(f"dataset {os.path.basename(path)}: l={l} records, hash table size s={s}")
        return header, records, s

    # ------------------------------------------------------------------
    # register
    # ------------------------------------------------------------------
    def do_register(self, name, ip, m_port, p_port):
        if self.m_sock is not None:
            print(f"already registered as {self.name}; start another peer process "
                  f"to register a different name")
            return
        self.name, self.ip = name, ip
        self.m_port, self.p_port = int(m_port), int(p_port)

        m_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        m_sock.bind(("0.0.0.0", self.m_port))
        m_sock.settimeout(30)
        p_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:  # bigger receive buffer: the leader streams many store messages
            p_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
        except OSError:
            pass
        p_sock.bind(("0.0.0.0", self.p_port))
        self.m_sock, self.p_sock = m_sock, p_sock

        reply = self.ask_manager("register", self.name, self.ip,
                                 self.m_port, self.p_port)
        self.log(f"register -> {reply[0]}"
                 + (f" ({reply[1]})" if reply[0] != SUCCESS and len(reply) > 1 else ""))
        if reply[0] == SUCCESS:
            threading.Thread(target=self.p_listener, daemon=True).start()
        else:
            # registration refused: release the ports so the user can retry
            m_sock.close()
            p_sock.close()
            self.m_sock = self.p_sock = None

    # ------------------------------------------------------------------
    # Ring-wide primitives (used by setup, leave, join and teardown)
    # ------------------------------------------------------------------
    def sync_ring(self):
        """Send a barrier token around the ring and wait for it to return.
        Every hop forwards messages in the order it received them, so once the
        token is back all earlier store messages have been applied.  Doubles
        as flow control: the leader never has more than BATCH_BYTES of
        records in flight, which keeps UDP receive buffers from overflowing."""
        with self.sync_lock:
            self.sync_seq += 1
            self.sync_wait_seq = self.sync_seq
            self.sync_event.clear()
            send_msg(self.p_sock, self.right_neighbor_addr, "sync", self.id, self.sync_seq)
            if not self.sync_event.wait(SYNC_TIMEOUT):
                self.log("WARNING: sync token did not come back (UDP loss?)")

    def teardown_ring(self, clear):
        """Start a teardown at this peer: each node deletes its local hash
        table as the message travels right around the ring; when it gets back
        here this peer deletes its own table (done in the listener)."""
        self.ev_teardown.clear()
        self.log(f"teardown -> {self.members[(self.id + 1) % self.n][0]} (around the ring)")
        send_msg(self.p_sock, self.right_neighbor_addr, "teardown", self.id, 1 if clear else 0)
        if not self.ev_teardown.wait(STEP_TIMEOUT):
            raise RuntimeError("teardown did not complete around the ring")

    def reset_ring(self, first_id, new_n, new_members):
        """Renumber the ring: node k after this peer gets identifier
        first_id + k, ring size new_n and the membership new_members.  The
        message is passed along the OLD ring (so it also reaches a leaving
        peer) and returns to this peer."""
        self.ev_reset.clear()
        send_msg(self.p_sock, self.right_neighbor_addr, "reset-id", self.name,
                 first_id, new_n, encode_tuples(new_members))
        if not self.ev_reset.wait(STEP_TIMEOUT):
            raise RuntimeError("reset-id did not come back around the ring")

    def construct_local_dhts(self, records):
        """Step 2 of 1.2.1: populate the DHT.  The leader (id 0) streams the
        records through the ring; a record is never sent straight to the
        node that will hold it."""
        counts = {i: 0 for i in range(self.n)}
        inflight = 0
        skipped = 0
        for line in records:
            try:
                event_id = int(line.split(",")[EVENT_ID_COL])
            except (ValueError, IndexError):
                skipped += 1      # not a well-formed event line; ignore it
                continue
            pos = hash_position(event_id, self.s)
            node_id = hash_node(pos, self.n)
            counts[node_id] += 1
            if node_id == self.id:
                self.table_add(pos, line)
            else:
                send_msg(self.p_sock, self.right_neighbor_addr,
                         "store", node_id, pos, line)
                inflight += len(line)
                if inflight >= BATCH_BYTES:
                    self.sync_ring()
                    inflight = 0
        self.sync_ring()
        if skipped:
            self.log(f"WARNING: skipped {skipped} malformed line(s) in the dataset")
        self.log(f"records stored per node id: {counts}")
        return counts

    # ------------------------------------------------------------------
    # setup-dht (this peer becomes the leader if the manager says SUCCESS)
    # ------------------------------------------------------------------
    def do_setup_dht(self, n, yyyy, csv_path=None):
        n = int(n)
        # Read the dataset first so a missing file is reported before the
        # manager's state is changed.
        header, records, s = self.load_dataset(self.find_dataset(yyyy, csv_path))

        reply = self.ask_manager("setup-dht", self.name, n, yyyy)
        self.log(f"setup-dht -> {reply[0]}")
        if reply[0] != SUCCESS:
            self.log(f"reason: {reply[1] if len(reply) > 1 else ''}")
            return

        members = decode_tuples(reply[2])
        self.s, self.yyyy, self.header = s, int(yyyy), header
        self.table_clear()
        self.set_ring(0, n, members)
        self.log(f"I am the leader (id=0). Ring order: {[m[0] for m in members]}")

        # Step 1: assign identifiers / neighbours (direct leader -> peer_i)
        for i in range(1, n):
            pname, pip, pport = members[i]
            send_msg(self.ctrl_sock, (pip, pport), "set-id", i, n, s, yyyy,
                     encode_tuples(members), header)
            ack, _ = recv_msg(self.ctrl_sock)
            self.log(f"set-id -> {pname} (id={i}) ack={ack}")

        # Step 2: construct the local DHTs; Step 3: signal completion
        self.construct_local_dhts(records)
        reply = self.ask_manager("dht-complete", self.name)
        self.log(f"dht-complete -> {reply}")

    # ------------------------------------------------------------------
    # query-dht <event-id>
    # ------------------------------------------------------------------
    def do_query_dht(self, event_id):
        event_id = int(event_id)
        reply = self.ask_manager("query-dht", self.name)
        if reply[0] != SUCCESS:
            self.log(f"query-dht -> FAILURE ({reply[1] if len(reply) > 1 else ''})")
            return
        target = decode_tuples(reply[1])[0]
        self.log(f"query-dht -> SUCCESS; start at {target[0]} ({target[1]}:{target[2]})")

        self.ev_query.clear()
        self.query_result = None
        self.log(f"find-event {event_id} -> {target[0]}")
        send_msg(self.p_sock, (target[1], target[2]), "find-event", event_id,
                 encode_tuples([self.my_tuple()]), "")
        if not self.ev_query.wait(STEP_TIMEOUT):
            self.log("no response to find-event (timed out)")
            return

        res = self.query_result
        if res[0] == SUCCESS:
            id_seq, header, record = res[1], res[2], tail(res, 3)
            self.log(f"Storm event {event_id} found. id-seq = [{id_seq}]")
            names = next(csv.reader([header]))
            values = next(csv.reader([record]))
            width = max(len(x) for x in names)
            for k, v in zip(names, values):
                print(f"    {k:<{width}} : {v}")
        else:
            self.log(f"Storm event {event_id} not found in the DHT.  "
                     f"id-seq = [{res[2] if len(res) > 2 else ''}]")

    # ------------------------------------------------------------------
    # leave-dht
    # ------------------------------------------------------------------
    def do_leave_dht(self):
        reply = self.ask_manager("leave-dht", self.name)
        self.log(f"leave-dht -> {reply[0]}"
                 + (f" ({reply[1]})" if reply[0] != SUCCESS and len(reply) > 1 else ""))
        if reply[0] != SUCCESS:
            return
        i, n, yyyy = self.id, self.n, self.yyyy

        # 1. tear the DHT down (this peer deletes its table when it returns)
        self.teardown_ring(clear=False)

        # 2. renumber: my right neighbour becomes id 0; I am removed
        new_members = [self.members[(i + 1 + k) % n] for k in range(n - 1)]
        self.log(f"reset-id: new ring {[m[0] for m in new_members]} "
                 f"(size {n - 1}); {new_members[0][0]} becomes the leader")
        self.reset_ring(0, n - 1, new_members)

        # 3. ask the new leader to rebuild the DHT
        new_leader = new_members[0]
        self.ev_rebuild.clear()
        self.rebuild_status = None
        self.log(f"rebuild-dht -> {new_leader[0]}")
        send_msg(self.p_sock, (new_leader[1], new_leader[2]), "rebuild-dht",
                 yyyy, encode_tuples([self.my_tuple()]))
        if not self.ev_rebuild.wait(REBUILD_TIMEOUT) or self.rebuild_status != SUCCESS:
            self.log("rebuild did not complete -- not reporting dht-rebuilt")
            return
        self.log(f"rebuild-done received from {new_leader[0]}")

        # 4. I am no longer part of the ring; tell the manager
        self.clear_ring()
        self.table_clear()
        reply = self.ask_manager("dht-rebuilt", self.name, new_leader[0])
        self.log(f"dht-rebuilt (new leader {new_leader[0]}) -> {reply}")

    # ------------------------------------------------------------------
    # join-dht
    # ------------------------------------------------------------------
    def do_join_dht(self):
        reply = self.ask_manager("join-dht", self.name)
        self.log(f"join-dht -> {reply[0]}"
                 + (f" ({reply[1]})" if reply[0] != SUCCESS and len(reply) > 1 else ""))
        if reply[0] != SUCCESS:
            return
        leader = decode_tuples(reply[1])[0]
        self.ev_rebuild.clear()
        self.rebuild_status = None
        self.log(f"join-request -> leader {leader[0]}")
        send_msg(self.p_sock, (leader[1], leader[2]), "join-request",
                 encode_tuples([self.my_tuple()]))
        if not self.ev_rebuild.wait(REBUILD_TIMEOUT) or self.rebuild_status != SUCCESS:
            self.log("rebuild did not complete -- not reporting dht-rebuilt")
            return
        self.log(f"rebuild-done received from leader {leader[0]}; I am node id {self.id}")
        reply = self.ask_manager("dht-rebuilt", self.name, leader[0])
        self.log(f"dht-rebuilt (leader {leader[0]}) -> {reply}")

    # ------------------------------------------------------------------
    # teardown-dht (leader only)
    # ------------------------------------------------------------------
    def do_teardown_dht(self):
        reply = self.ask_manager("teardown-dht", self.name)
        self.log(f"teardown-dht -> {reply[0]}"
                 + (f" ({reply[1]})" if reply[0] != SUCCESS and len(reply) > 1 else ""))
        if reply[0] != SUCCESS:
            return
        self.teardown_ring(clear=True)
        self.table_clear()
        self.clear_ring()
        reply = self.ask_manager("teardown-complete", self.name)
        self.log(f"teardown-complete -> {reply}")

    # ------------------------------------------------------------------
    # deregister
    # ------------------------------------------------------------------
    def do_deregister(self):
        reply = self.ask_manager("deregister", self.name)
        self.log(f"deregister -> {reply[0]}"
                 + (f" ({reply[1]})" if reply[0] != SUCCESS and len(reply) > 1 else ""))
        return reply[0] == SUCCESS

    # ==================================================================
    # P2P listener thread
    # ==================================================================
    def p_listener(self):
        while True:
            try:
                fields, addr = recv_msg(self.p_sock)
                self.handle_p2p(fields, addr)
            except Exception as e:
                self.log(f"(p2p) error: {e}")

    def handle_p2p(self, fields, addr):
        cmd = fields[0]

        if cmd == "set-id":      # set-id|id|n|s|yyyy|tuples|header
            node_id, n, s, yyyy = int(fields[1]), int(fields[2]), int(fields[3]), int(fields[4])
            members = decode_tuples(fields[5])
            self.s, self.yyyy, self.header = s, yyyy, tail(fields, 6)
            self.table_clear()
            self.set_ring(node_id, n, members)
            self.log(f"set-id received: id={node_id} n={n}; right neighbor="
                     f"{members[(node_id + 1) % n][0]}")
            send_msg(self.p_sock, addr, SUCCESS)

        elif cmd == "store":     # store|target-id|pos|record
            target_id, pos, record = int(fields[1]), int(fields[2]), tail(fields, 3)
            if target_id == self.id:
                self.table_add(pos, record)
            else:
                send_msg(self.p_sock, self.right_neighbor_addr,
                         "store", target_id, pos, record)

        elif cmd == "sync":      # sync|origin-id|seq
            origin, seq = int(fields[1]), int(fields[2])
            if self.id == origin:
                if seq == self.sync_wait_seq:
                    self.sync_event.set()
            else:
                send_msg(self.p_sock, self.right_neighbor_addr, "sync", origin, seq)

        elif cmd == "teardown":  # teardown|origin-id|clear
            origin, clear = int(fields[1]), fields[2] == "1"
            self.table_clear()
            if self.id == origin:
                self.log("teardown came back around the ring; local hash table deleted")
                self.ev_teardown.set()
            else:
                self.log("teardown: local hash table deleted; forwarding to right neighbor")
                send_msg(self.p_sock, self.right_neighbor_addr, "teardown", origin, int(clear))
                if clear:
                    self.clear_ring()

        elif cmd == "reset-id":  # reset-id|origin|new-id|new-n|tuples
            origin, new_id, new_n = fields[1], int(fields[2]), int(fields[3])
            if self.name == origin:
                self.ev_reset.set()
                return
            old_right = self.right_neighbor_addr
            self.set_ring(new_id, new_n, decode_tuples(fields[4]))
            self.log(f"reset-id received: now id={new_id}, ring size {new_n}")
            send_msg(self.p_sock, old_right, "reset-id", origin, new_id + 1, new_n, fields[4])

        elif cmd == "rebuild-dht":   # rebuild-dht|yyyy|leaver-tuple   (new leader)
            leaver = decode_tuples(fields[2])[0]
            self.log(f"rebuild-dht received from {leaver[0]}: I am the new leader")
            threading.Thread(target=self.rebuild_as_new_leader,
                             args=(int(fields[1]), leaver), daemon=True).start()

        elif cmd == "join-request":  # join-request|joiner-tuple         (leader)
            joiner = decode_tuples(fields[1])[0]
            self.log(f"join-request received from {joiner[0]}")
            threading.Thread(target=self.admit_joiner,
                             args=(joiner,), daemon=True).start()

        elif cmd == "rebuild-done":  # rebuild-done|SUCCESS or FAILURE
            self.rebuild_status = fields[1]
            self.ev_rebuild.set()

        elif cmd == "find-event":    # find-event|event-id|S-tuple|id-seq
            self.on_find_event(fields)

        elif cmd == "find-event-response":
            self.query_result = fields[1:]
            self.ev_query.set()

        else:
            self.log(f"(p2p) unsupported command: {cmd}")

    # ------------------------------------------------------------------
    # find-event: hot-potato query processing (1.2.2)
    # ------------------------------------------------------------------
    def on_find_event(self, fields):
        event_id = int(fields[1])
        querier = decode_tuples(fields[2])[0]
        id_seq = [int(x) for x in fields[3].split(",")] if fields[3] else []
        id_seq.append(self.id)
        seq_txt = ",".join(str(x) for x in id_seq)

        pos = hash_position(event_id, self.s)
        owner = hash_node(pos, self.n)
        with self.lock:
            candidates = list(self.table.get(pos, []))
        hit = next((r for r in candidates
                    if r.split(",")[EVENT_ID_COL] == str(event_id)), None)
        self.log(f"find-event {event_id}: pos={pos}, owner id={owner}, id-seq=[{seq_txt}]; "
                 f"{'FOUND here' if hit else 'not in my table'}")

        q_addr = (querier[1], querier[2])
        if hit is not None:
            send_msg(self.p_sock, q_addr, "find-event-response", SUCCESS, seq_txt,
                     self.header, hit)
            return

        remaining = [i for i in range(self.n) if i not in id_seq]
        if owner == self.id or not remaining:
            # the owner does not hold it (or everyone was probed): not in DHT
            send_msg(self.p_sock, q_addr, "find-event-response", FAILURE,
                     event_id, seq_txt)
            return

        nxt = random.choice(remaining)           # hot potato: random next node
        m = self.members[nxt]
        self.log(f"find-event {event_id}: forwarding hot potato to id {nxt} ({m[0]})")
        send_msg(self.p_sock, (m[1], m[2]), "find-event", event_id, fields[2], seq_txt)

    # ------------------------------------------------------------------
    # Worker threads started by the listener
    # ------------------------------------------------------------------
    def rebuild_as_new_leader(self, yyyy, leaver):
        """After leave-dht: this peer is now id 0 of the smaller ring and
        repeats step 2 of 1.2.1, then signals the leaver."""
        status = SUCCESS
        try:
            header, records, s = self.load_dataset(self.find_dataset(yyyy))
            self.s, self.header, self.yyyy = s, header, yyyy
            self.table_clear()
            self.construct_local_dhts(records)
        except Exception as e:
            self.log(f"rebuild failed: {e}")
            status = FAILURE
        self.log(f"rebuild-done -> {leaver[0]} ({status})")
        send_msg(self.p_sock, (leaver[1], leaver[2]), "rebuild-done", status)

    def admit_joiner(self, joiner):
        """Leader side of join-dht: the joiner is appended at the end of the
        ring (identifier n), the old ring is torn down and renumbered with the
        new size, the joiner is given its identifier, and the DHT is rebuilt."""
        status = SUCCESS
        try:
            old_n, old_members = self.n, list(self.members)
            new_members = old_members + [joiner]
            self.teardown_ring(clear=False)
            self.reset_ring(1, old_n + 1, new_members)   # ids stay as they were
            self.set_ring(0, old_n + 1, new_members)

            self.log(f"set-id -> {joiner[0]} (id={old_n})")
            send_msg(self.ctrl_sock, (joiner[1], joiner[2]), "set-id", old_n,
                     old_n + 1, self.s, self.yyyy, encode_tuples(new_members), self.header)
            ack, _ = recv_msg(self.ctrl_sock)
            self.log(f"set-id -> {joiner[0]} ack={ack}")

            _, records, _ = self.load_dataset(self.find_dataset(self.yyyy))
            self.construct_local_dhts(records)
        except Exception as e:
            self.log(f"join failed: {e}")
            status = FAILURE
        self.log(f"rebuild-done -> {joiner[0]} ({status})")
        send_msg(self.p_sock, (joiner[1], joiner[2]), "rebuild-done", status)

    # ------------------------------------------------------------------
    def do_show_table(self):
        self.log(f"id={self.id} ring size={self.n}: local table holds "
                 f"{self.table_count()} record(s)")

    # ------------------------------------------------------------------
    def repl(self):
        print(f"peer ready. Manager at {self.manager_addr}. Commands: "
              f"register <name> <ip> <m-port> <p-port> | setup-dht <n> <YYYY> [csv] | "
              f"query-dht <event-id> | leave-dht | join-dht | teardown-dht | "
              f"deregister | show-table | quit")
        while True:
            try:
                line = input("> ").strip()
            except EOFError:
                break
            if not line:
                continue
            parts = line.split()
            cmd, args = parts[0], parts[1:]
            try:
                if cmd == "register":
                    self.do_register(*args[:4])
                elif cmd == "setup-dht":
                    self.do_setup_dht(*args[:3])
                elif cmd == "query-dht":
                    self.do_query_dht(*args[:1])
                elif cmd == "leave-dht":
                    self.do_leave_dht()
                elif cmd == "join-dht":
                    self.do_join_dht()
                elif cmd == "teardown-dht":
                    self.do_teardown_dht()
                elif cmd == "deregister":
                    if self.do_deregister():
                        self.log("deregistered; exiting")
                        break
                elif cmd == "show-table":
                    self.do_show_table()
                elif cmd == "quit":
                    break
                else:
                    print(f"unknown command: {cmd}")
            except Exception as e:
                print(f"error: {e}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("usage: python3 peer.py <manager-ip> <manager-port>")
        sys.exit(1)
    Peer(sys.argv[1], int(sys.argv[2])).repl()

"""
manager.py -- DHT Manager (full project)

Usage:
    python3 manager.py <port>

Always-on process that keeps the state information base for the DHT
application and answers every command a peer can send it (project
description, section 1.1):

    register, setup-dht, dht-complete, query-dht, leave-dht, join-dht,
    dht-rebuilt, deregister, teardown-dht, teardown-complete

State kept per peer:   name -> {"ip", "m_port", "p_port", "state"}
    state is one of "Free", "Leader", "InDHT"

DHT state (self.dht), None while no DHT exists:
    {
        "leader":  <peer-name of the current leader (ring identifier 0)>,
        "n":       <ring size>,
        "yyyy":    <dataset year>,
        "members": [(name, ip, p_port), ...],   # current members of the ring
    }

"Waiting" state (self.waiting), None normally.  Several commands start a
multi-step exchange among peers that the manager is not part of (building,
rebuilding or deleting the DHT).  While that is going on the manager must
answer FAILURE to every message except the one that ends the exchange:

    {"cmd": <the only command accepted>, "peer": <who must send it>,
     "op": "leave" | "join"   # only for dht-rebuilt}
"""

import random
import socket
import sys

from protocol import (
    SUCCESS, FAILURE, send_msg, recv_msg, encode_tuples,
)


class Manager:
    def __init__(self, port: int):
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", port))
        self.peers = {}      # name -> dict(ip, m_port, p_port, state)
        self.dht = None
        self.waiting = None

    # ------------------------------------------------------------------
    # small helpers
    # ------------------------------------------------------------------
    def reply(self, addr, *fields):
        send_msg(self.sock, addr, *fields)

    def fail(self, addr, reason):
        print(f"[manager]   -> FAILURE ({reason})")
        self.reply(addr, FAILURE, reason)

    def print_state(self):
        summary = ", ".join(f"{n}={p['state']}" for n, p in self.peers.items())
        print(f"[manager]   state: {summary if summary else '(no peers)'}")

    def member_tuple(self, name):
        p = self.peers[name]
        return (name, p["ip"], p["p_port"])

    # ------------------------------------------------------------------
    # 1. register <peer-name> <IPv4-address> <m-port> <p-port>
    # ------------------------------------------------------------------
    def handle_register(self, fields, addr):
        if len(fields) != 5:
            return self.fail(addr, "malformed register")
        _, name, ip, m_port, p_port = fields
        if not (m_port.isdigit() and p_port.isdigit()):
            return self.fail(addr, "ports must be integers")
        m_port, p_port = int(m_port), int(p_port)
        if len(name) > 15 or not name.isalpha():
            return self.fail(addr, "invalid peer-name")
        if name in self.peers:
            return self.fail(addr, "duplicate registration")
        if m_port == p_port:
            return self.fail(addr, "m-port and p-port must differ")
        used = {p["m_port"] for p in self.peers.values()} | \
               {p["p_port"] for p in self.peers.values()}
        if m_port in used or p_port in used:
            return self.fail(addr, "port already in use")
        self.peers[name] = {"ip": ip, "m_port": m_port, "p_port": p_port,
                            "state": "Free"}
        print(f"[manager] register: {name} at {ip} m-port={m_port} p-port={p_port}")
        self.reply(addr, SUCCESS)
        self.print_state()

    # ------------------------------------------------------------------
    # 2. setup-dht <peer-name> <n> <YYYY>
    # ------------------------------------------------------------------
    def handle_setup_dht(self, fields, addr):
        if len(fields) != 4:
            return self.fail(addr, "malformed setup-dht")
        _, name, n_str, yyyy = fields
        if not (n_str.isdigit() and yyyy.isdigit()):
            return self.fail(addr, "n and YYYY must be integers")
        n = int(n_str)

        if name not in self.peers:
            return self.fail(addr, "peer not registered")
        if n < 3:
            return self.fail(addr, "n must be >= 3")
        free = [p for p, v in self.peers.items() if v["state"] == "Free"]
        if len(self.peers) < n or len(free) < n:
            return self.fail(addr, "not enough free peers registered")
        if self.dht is not None:
            return self.fail(addr, "a DHT already exists")
        if self.peers[name]["state"] != "Free":
            return self.fail(addr, "leader is not Free")

        chosen = random.sample([p for p in free if p != name], n - 1)
        self.peers[name]["state"] = "Leader"
        for p in chosen:
            self.peers[p]["state"] = "InDHT"

        members = [self.member_tuple(name)] + [self.member_tuple(p) for p in chosen]
        self.dht = {"leader": name, "n": n, "yyyy": int(yyyy), "members": members}
        self.waiting = {"cmd": "dht-complete", "peer": name}
        print(f"[manager] setup-dht: leader={name} n={n} yyyy={yyyy} "
              f"ring={[m[0] for m in members]}")
        self.reply(addr, SUCCESS, n, encode_tuples(members))
        self.print_state()

    # ------------------------------------------------------------------
    # 3. dht-complete <peer-name>
    # ------------------------------------------------------------------
    def handle_dht_complete(self, fields, addr):
        if len(fields) != 2:
            return self.fail(addr, "malformed dht-complete")
        name = fields[1]
        if self.dht is None or name != self.dht["leader"]:
            return self.fail(addr, "not the leader of the DHT")
        self.waiting = None
        print(f"[manager] dht-complete from leader {name}: DHT of size "
              f"{self.dht['n']} is ready")
        self.reply(addr, SUCCESS)

    # ------------------------------------------------------------------
    # 4. query-dht <peer-name>
    # ------------------------------------------------------------------
    def handle_query_dht(self, fields, addr):
        if len(fields) != 2:
            return self.fail(addr, "malformed query-dht")
        name = fields[1]
        if self.dht is None:
            return self.fail(addr, "no DHT exists")
        if name not in self.peers:
            return self.fail(addr, "peer not registered")
        if self.peers[name]["state"] != "Free":
            return self.fail(addr, "peer is not Free")
        target = random.choice(self.dht["members"])
        print(f"[manager] query-dht from {name}: start the query at {target[0]}")
        self.reply(addr, SUCCESS, encode_tuples([target]))

    # ------------------------------------------------------------------
    # 5. leave-dht <peer-name>
    # ------------------------------------------------------------------
    def handle_leave_dht(self, fields, addr):
        if len(fields) != 2:
            return self.fail(addr, "malformed leave-dht")
        name = fields[1]
        if self.dht is None:
            return self.fail(addr, "no DHT exists")
        if name not in self.peers or self.peers[name]["state"] not in ("Leader", "InDHT"):
            return self.fail(addr, "peer is not maintaining the DHT")
        self.waiting = {"cmd": "dht-rebuilt", "peer": name, "op": "leave"}
        print(f"[manager] leave-dht: {name} is leaving; waiting for dht-rebuilt")
        self.reply(addr, SUCCESS)

    # ------------------------------------------------------------------
    # 6. join-dht <peer-name>
    #    Extension of the SUCCESS reply: it also carries the leader's
    #    3-tuple, which the joining peer needs to contact the leader.
    # ------------------------------------------------------------------
    def handle_join_dht(self, fields, addr):
        if len(fields) != 2:
            return self.fail(addr, "malformed join-dht")
        name = fields[1]
        if self.dht is None:
            return self.fail(addr, "no DHT exists")
        if name not in self.peers or self.peers[name]["state"] != "Free":
            return self.fail(addr, "peer is not Free")
        self.waiting = {"cmd": "dht-rebuilt", "peer": name, "op": "join"}
        leader = self.member_tuple(self.dht["leader"])
        print(f"[manager] join-dht: {name} is joining via leader "
              f"{leader[0]}; waiting for dht-rebuilt")
        self.reply(addr, SUCCESS, encode_tuples([leader]))

    # ------------------------------------------------------------------
    # 7. dht-rebuilt <peer-name> <new-leader>
    # ------------------------------------------------------------------
    def handle_dht_rebuilt(self, fields, addr):
        if len(fields) != 3:
            return self.fail(addr, "malformed dht-rebuilt")
        _, name, new_leader = fields
        w = self.waiting
        if w is None or w["cmd"] != "dht-rebuilt" or w["peer"] != name:
            return self.fail(addr, "not the peer that started the rebuild")

        members = list(self.dht["members"])
        if w["op"] == "leave":
            members = [m for m in members if m[0] != name]
            self.peers[name]["state"] = "Free"
        else:  # join
            members.append(self.member_tuple(name))
        if new_leader not in [m[0] for m in members]:
            return self.fail(addr, "new-leader is not a member of the DHT")

        # new leader first, remaining members after (ring order is kept by
        # the peers themselves; the manager only needs the set + leader)
        members.sort(key=lambda m: m[0] != new_leader)
        for m in members:
            self.peers[m[0]]["state"] = "Leader" if m[0] == new_leader else "InDHT"
        self.dht.update(leader=new_leader, n=len(members), members=members)
        self.waiting = None
        print(f"[manager] dht-rebuilt from {name} ({w['op']}): new leader="
              f"{new_leader}, ring size={len(members)}")
        self.reply(addr, SUCCESS)
        self.print_state()

    # ------------------------------------------------------------------
    # 8. deregister <peer-name>
    # ------------------------------------------------------------------
    def handle_deregister(self, fields, addr):
        if len(fields) != 2:
            return self.fail(addr, "malformed deregister")
        name = fields[1]
        if name not in self.peers:
            return self.fail(addr, "peer not registered")
        if self.peers[name]["state"] != "Free":
            return self.fail(addr, "peer is part of the DHT")
        del self.peers[name]
        print(f"[manager] deregister: {name} removed")
        self.reply(addr, SUCCESS)
        self.print_state()

    # ------------------------------------------------------------------
    # 9. teardown-dht <peer-name>
    # ------------------------------------------------------------------
    def handle_teardown_dht(self, fields, addr):
        if len(fields) != 2:
            return self.fail(addr, "malformed teardown-dht")
        name = fields[1]
        if self.dht is None or name != self.dht["leader"]:
            return self.fail(addr, "not the leader of the DHT")
        self.waiting = {"cmd": "teardown-complete", "peer": name}
        print(f"[manager] teardown-dht from leader {name}; waiting for teardown-complete")
        self.reply(addr, SUCCESS)

    # ------------------------------------------------------------------
    # 10. teardown-complete <peer-name>
    # ------------------------------------------------------------------
    def handle_teardown_complete(self, fields, addr):
        if len(fields) != 2:
            return self.fail(addr, "malformed teardown-complete")
        name = fields[1]
        if self.dht is None or name != self.dht["leader"]:
            return self.fail(addr, "not the leader of the DHT")
        for m in self.dht["members"]:
            self.peers[m[0]]["state"] = "Free"
        self.dht = None
        self.waiting = None
        print(f"[manager] teardown-complete from {name}: DHT deleted, all members Free")
        self.reply(addr, SUCCESS)
        self.print_state()

    # ------------------------------------------------------------------
    HANDLERS = {
        "register": handle_register,
        "setup-dht": handle_setup_dht,
        "dht-complete": handle_dht_complete,
        "query-dht": handle_query_dht,
        "leave-dht": handle_leave_dht,
        "join-dht": handle_join_dht,
        "dht-rebuilt": handle_dht_rebuilt,
        "deregister": handle_deregister,
        "teardown-dht": handle_teardown_dht,
        "teardown-complete": handle_teardown_complete,
    }

    def dispatch(self, fields, addr):
        cmd = fields[0]

        # While a multi-step exchange among peers is outstanding, the only
        # message accepted is the one that finishes it; everything else gets
        # FAILURE (project description, 1.1 items 2, 5, 6 and 9).
        if self.waiting is not None and cmd != self.waiting["cmd"]:
            return self.fail(addr, f"busy: waiting for {self.waiting['cmd']}")

        handler = self.HANDLERS.get(cmd)
        if handler is None:
            return self.fail(addr, f"unknown command: {cmd}")
        handler(self, fields, addr)

    def run(self):
        print(f"[manager] listening on UDP port {self.port}")
        while True:
            fields, addr = recv_msg(self.sock)
            print(f"[manager] recv from {addr[0]}:{addr[1]}: {'|'.join(fields)}")
            try:
                self.dispatch(fields, addr)
            except Exception as e:  # never let a bad datagram kill the manager
                print(f"[manager] error handling {fields}: {e}")
                send_msg(self.sock, addr, FAILURE, "internal error")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: python3 manager.py <port>")
        sys.exit(1)
    Manager(int(sys.argv[1])).run()

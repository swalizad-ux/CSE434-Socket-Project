"""
protocol.py
Shared message format and helper functions for the DHT Manager / Peer / P2P protocol.

Message format: fields are separated by '|'. The first field is always the
command name. List-of-tuples fields (used for the n 3-tuples of
peer-name/IP/p-port) are encoded as a single field with tuples separated
by ';' and the elements of each tuple separated by ','.

Example encode of a SUCCESS reply to setup-dht with 3 peers:
    SUCCESS|3|leaderName,127.0.0.1,1501;peer1,127.0.0.1,1502;peer2,127.0.0.1,1503

Design rationale (see design document): a plain-text, '|'-delimited format
was chosen over a binary struct because it is trivial to log, trivial to
debug with a plain print of the datagram payload, and small enough (well
under the 65507-byte UDP payload limit) for every message this project
defines -- register/setup-dht/dht-complete messages carry at most a few
hundred bytes even for the largest n we test with.
"""

import socket

BUFSZ = 65535
ENCODING = "utf-8"

SUCCESS = "SUCCESS"
FAILURE = "FAILURE"


def encode(*fields):
    """Join fields with '|' into a single UDP payload (bytes)."""
    return "|".join(str(f) for f in fields).encode(ENCODING)


def decode(data: bytes):
    """Split a UDP payload back into a list of string fields."""
    return data.decode(ENCODING).split("|")


def encode_tuples(tuples):
    """Encode a list of (name, ip, port) tuples into a single field."""
    return ";".join(f"{name},{ip},{port}" for (name, ip, port) in tuples)


def decode_tuples(field):
    """Decode a field produced by encode_tuples back into a list of tuples."""
    if field == "":
        return []
    out = []
    for chunk in field.split(";"):
        name, ip, port = chunk.split(",")
        out.append((name, ip, int(port)))
    return out


def tail(fields, start):
    """Re-join fields[start:] with '|'.  Used for the last field of a
    message that carries free text (a raw CSV record or the CSV header),
    so such text survives even if it were ever to contain a '|'."""
    return "|".join(fields[start:])


def send_msg(sock: socket.socket, addr, *fields):
    sock.sendto(encode(*fields), addr)


def recv_msg(sock: socket.socket):
    data, addr = sock.recvfrom(BUFSZ)
    return decode(data), addr


# ---------------------------------------------------------------------------
# Hashing helpers used to build/consult the DHT (see design document, section
# "Data structures / hashing" and the project description, section 1.2.1).
# ---------------------------------------------------------------------------

def is_prime(x: int) -> bool:
    if x < 2:
        return False
    if x in (2, 3):
        return True
    if x % 2 == 0:
        return False
    i = 3
    while i * i <= x:
        if x % i == 0:
            return False
        i += 2
    return True


def first_prime_greater_than(n: int) -> int:
    """First prime strictly greater than n."""
    candidate = n + 1
    while not is_prime(candidate):
        candidate += 1
    return candidate


def hash_position(event_id: int, table_size: int) -> int:
    """pos = event_id mod s"""
    return event_id % table_size


def hash_node(pos: int, ring_size: int) -> int:
    """id = pos mod n"""
    return pos % ring_size

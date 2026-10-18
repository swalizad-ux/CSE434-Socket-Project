# Socket Project (Full) -- Group 1

Distributed hash table (ring) of storm events with a manager and peers, in
Python 3 (standard library only), over UDP. Group 1 port range: 1500-1999.

## Files
- `manager.py`  -- DHT manager. Run: `python3 manager.py <port>`
- `peer.py`     -- DHT peer.    Run: `python3 peer.py <manager-ip> <manager-port>`
- `protocol.py` -- shared message encode/decode and hashing helpers.
- `tests/`      -- integration test: `gen_csv.py` makes a synthetic NOAA-format
                   file, `run_scenario.py` drives a real manager + 6 peers.
- `Design_Document.pdf` -- design document (message formats, time-space
                   diagrams, design decisions, version-control proof, video link).

## Peer commands (typed on stdin)
    register <peer-name> <my-ip> <m-port> <p-port>
    setup-dht <n> <YYYY> [csv-path]     this peer becomes the leader
    query-dht <event-id>
    leave-dht
    join-dht
    teardown-dht                        leader only
    deregister                          Free peers only; the process exits
    show-table                          debug: records held locally
    quit

The manager commands dht-complete, dht-rebuilt and teardown-complete are
sent automatically by the peer at the end of setup-dht, leave/join-dht and
teardown-dht.

## Dataset
`details-YYYY.csv` (NOAA storm events, header on the first line) is not
included. **Copy `details-YYYY.csv` into the directory each peer is started
in, on every host**: any peer can become the leader after a `leave-dht`, and
the leader reads the file to rebuild the DHT. (`setup-dht` also accepts an
explicit path as its third argument.)

## Example: one machine (for local testing)
    python3 manager.py 1500                           # terminal 1
    python3 peer.py 127.0.0.1 1500                    # terminals 2..7
    > register alice 127.0.0.1 1501 1502              # ports in 1500-1999,
    > register bob   127.0.0.1 1503 1504              # all distinct
    ...
    > setup-dht 5 1996                                # on one peer
    > query-dht 5536849                               # on a peer outside the DHT
    > leave-dht / join-dht / teardown-dht / deregister

## Several hosts
Start the manager on one host, and pass that host's real IP as `<manager-ip>`
to every peer. When registering, give each peer *its own host's* real IP as
`<my-ip>` (not 127.0.0.1). Use distinct ports for every m-port and p-port.

## Message formats
See section 1 of the design document. Fields are '|'-separated plain text.
P2P messages: set-id, store, sync, teardown, reset-id, rebuild-dht,
join-request, rebuild-done, find-event, find-event-response.

## Running the integration test
    cd tests
    python3 gen_csv.py            # writes details-1996.csv (synthetic)
    python3 run_scenario.py       # uses ports 1500-1602 on localhost

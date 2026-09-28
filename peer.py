import json
import queue
import shlex
import socket
import sys
import threading
import time
from common import dataset, decode, ipv4, packet, port, prime_after, send, trace, udp_socket
TIMEOUT = 2.0           #Wait up to 2 seconds per attempt with 5 total attempts
ATTEMPTS = 5

class Peer:
#----Peer State-----
    def __init__(self, manager):
        self.manager = manager
        self.name = None
        self.manager_socket = self.peer_socket = None       #Use separate sockets for manager traffic and peer traffic
        self.node_id = None
        self.ring_peers = []
        self.next_peer = None
        self.table_size = 0
        self.hash_table = []
        self.saved_rows = set()         #Track stored row numbers so retries do not insert duplicates
        self.waiting_replies = {}
        self.lock = threading.Lock()
        self.build_id = None
        self.finished = False

#------Requests to manager------
    def manager_request(self, message):
        for _ in range(ATTEMPTS):
            send(self.manager_socket, message, self.manager)
            deadline = time.monotonic() + TIMEOUT       #Unrelated messages should not extend the timeout.
            while time.monotonic() < deadline:
                self.manager_socket.settimeout(max(0.001, deadline - time.monotonic()))
                try:
                    data, source = self.manager_socket.recvfrom(65535)
                except socket.timeout:
                    break
                try:
                    reply = decode(data)
                except (ValueError, UnicodeError):
                    continue
               #Match the manager port and transaction ID befor accepting a reply
                if source[1] == self.manager[1] and reply['cmd'] == 'response' and (reply['tx'] == message['tx']) and (reply.get('status') in ('SUCCESS', 'FAILURE')):
                    trace(f"RX manager {message['cmd']} {reply['status']} {reply.get('reason', '')}")
                    return reply
        raise TimeoutError('Manager unavailable; outcome unknown. Restart all processes before retrying setup.')

#----Register this peer-----
    def register(self, name, ip, manager_port, peer_port):
        if self.name is not None:
            raise ValueError('This process is already registered')
        ip, manager_port, peer_port = (ipv4(ip), port(manager_port), port(peer_port))
        manager_socket = udp_socket((ip, manager_port))
        #If the second socket cannot bind realease the first one
        try:
            peer_socket = udp_socket((ip, peer_port))
        except Exception:
            manager_socket.close()
            raise
        self.manager_socket, self.peer_socket = (manager_socket, peer_socket)
        try:
            reply = self.manager_request(packet('register', name=name, ip=ip, m_port=manager_port, p_port=peer_port))
            if reply['status'] != 'SUCCESS':
                raise ValueError(reply.get('reason', 'Registration failed'))
        except Exception:       #A falied registration shouild not leave the local ports occupied
            manager_socket.close()
            peer_socket.close()
            self.manager_socket = self.peer_socket = None
            raise
        self.name = name
        threading.Thread(target=self.listen, daemon=True).start()
        trace(f'REGISTERED {name} m-port={manager_port} p-port={peer_port}')

#----Set up ring membership-----
    def configure(self, message):
        ring_peers, node_id = (message['peers'], int(message['id']))
        if not isinstance(ring_peers, list) or not isinstance(message['build'], str):
            raise ValueError('Invalid ring configuration')
        for peer in ring_peers:
            if not isinstance(peer, list) or len(peer) != 3 or (not isinstance(peer[0], str)):
                raise ValueError('Invalid peer tuple')
            ipv4(peer[1])
            port(peer[2])
        if len({peer[0] for peer in ring_peers}) != len(ring_peers):
            raise ValueError('Duplicate peer names in ring')
        if len({tuple(peer[1:]) for peer in ring_peers}) != len(ring_peers):
            raise ValueError('Duplicate peer endpoints in ring')
        if len(ring_peers) != int(message['n']) or len(ring_peers) < 3:
            raise ValueError('Invalid ring size')
        if not 0 <= node_id < len(ring_peers) or ring_peers[node_id][0] != self.name:
            raise ValueError('Wrong identifier assignment')
        if self.build_id is not None:   #accept an identical retry without clearing records already stored
            if self.build_id != message['build'] or self.ring_peers != ring_peers or self.node_id != node_id:
                raise ValueError('Peer is already assigned to a ring')
            return
        self.ring_peers, self.node_id = (ring_peers, node_id)
        self.next_peer = ring_peers[(node_id + 1) % len(ring_peers)]
        self.build_id = message['build']
        self.table_size, self.hash_table, self.saved_rows = (0, [], set())
        trace(f'CONFIG name={self.name} id={node_id} n={len(ring_peers)} right={ring_peers[(node_id + 1) % len(ring_peers)]}')

#------Send and wait for peer replies----
    def forward(self, message):
        send(self.peer_socket, message, self.next_peer[1:])

    def exchange(self, message, address=None):
        reply_queue = queue.Queue()
        with self.lock:
            self.waiting_replies[message['tx']] = reply_queue
        try:
            for _ in range(ATTEMPTS):
                if address is None:
                    self.forward(message)
                else:
                    send(self.peer_socket, message, address)
                try:
                    result = reply_queue.get(timeout=TIMEOUT)
                    if result.get('status') == 'FAILURE':
                        raise ValueError(result.get('reason', 'Peer rejected message'))
                    return result
                except queue.Empty:
                    trace(f"RETRY {message['cmd']} tx={message['tx'][:8]}")
            raise TimeoutError(f"No completion for {message['cmd']}; restart all processes")
        finally:            #remove the waiting entry whether the request succeeds or fails
            with self.lock:
                self.waiting_replies.pop(message['tx'], None)

    def deliver(self, message):
        with self.lock:
            reply_queue = self.waiting_replies.get(message['tx'])
        if reply_queue is not None:
            reply_queue.put(message)


#-----Store a storm record-----
    def store(self, message):
        row = message['record']
        if not isinstance(row, list) or len(row) != 14 or (not all((isinstance(field, str) for field in row))) or (self.table_size <= 0) or (int(row[0]) < 0):
            raise ValueError('Invalid storm record')
        pos = int(row[0]) % self.table_size
        if pos % len(self.ring_peers) != self.node_id:
            raise ValueError('Record routed to wrong owner')
        index = int(message['row'])
        if index < 0:
            raise ValueError('Invalid row index')
        if index not in self.saved_rows:
            self.hash_table[pos].append(row)
            self.saved_rows.add(index)
            trace(f'STORED peer={self.name} id={self.node_id} row={index} event_id={row[0]} pos={pos}')

#----Handle incoming peer messages----
    def handle_p2p(self, message, source):
        command = message['cmd']
        if command == 'set-id':
            if int(message['id']) == 0:
                raise ValueError('Leader configures itself locally')
            if source != tuple(message['peers'][0][1:]):
                raise ValueError('set-id did not originate at leader')
            self.configure(message)
            send(self.peer_socket, dict(v=1, cmd='set-id-ack', tx=message['tx'], status='SUCCESS'), source)
            return
        if command == 'set-id-ack':
            if any((source == tuple(peer[1:]) for peer in self.ring_peers[1:])):
                self.deliver(message)
            return
        if not self.ring_peers or message.get('build') != self.build_id:
            raise ValueError('Unknown ring construction')
        if source != tuple(self.ring_peers[(self.node_id - 1) % len(self.ring_peers)][1:]):
            raise ValueError('Ring token must arrive from left neighbour')
        if self.node_id == 0:
            if command == 'store':
                raise ValueError('Protocol error: unconsumed store returned to leader')
            if command in ('init-table', 'stored', 'count'):
                self.deliver(message)
                return
            raise ValueError('Unknown token')
        if command == 'init-table':
            requested = int(message['s'])
            if requested < 2:
                raise ValueError('Invalid table size')
            if self.table_size == 0:
                self.table_size = requested
                self.hash_table = [[] for _ in range(self.table_size)]
            elif requested != self.table_size:
                raise ValueError('Conflicting table size')
        elif command == 'store':
            owner = int(message['record'][0]) % self.table_size % len(self.ring_peers)
            if owner == self.node_id:
                self.store(message)
                message = dict(v=1, cmd='stored', tx=message['tx'], build=self.build_id, row=message['row'], owner=self.node_id)
        elif command == 'count':
            message['counts'][str(self.node_id)] = len(self.saved_rows)
            trace(f'LOCAL COUNT peer={self.name} id={self.node_id} records={len(self.saved_rows)}')
        elif command != 'stored':
            raise ValueError('Unknown P2P command')
        self.forward(message)


#----Background recieve loop----
    def listen(self):
        while True:
            try:
                data, source = self.peer_socket.recvfrom(65535)
                message = decode(data)
                trace(f"RX {message['cmd']} tx={message['tx'][:8]} from={source}")
                self.handle_p2p(message, source)
            except (ValueError, KeyError, TypeError, IndexError, ZeroDivisionError, UnicodeError, OverflowError, OSError) as error:
                trace(f'IGNORE invalid P2P message: {error}')

#-----build the dht as leader-----
    def setup(self, name, n, year):
        if name != self.name:
            raise ValueError('Use this registered peer name')
        if self.finished or self.ring_peers:
            raise ValueError('A DHT already exists in this process')
        n, year = (int(n), int(year))
        records = dataset(year)
        s = prime_after(2 * len(records))
        for row in records:
            if len(json.dumps(row).encode('utf-8')) > 50000:
                raise ValueError('CSV record too large for a UDP store message')
        request = packet('setup-dht', name=name, n=n, year=year)
        result = self.manager_request(request)
        if result['status'] != 'SUCCESS':
            return
        ring_peers = result['peers']
        build = request['tx']
        self.configure(dict(id=0, n=n, peers=ring_peers, build=build))
        trace(f'DATASET details-{year}.csv records={len(records)} s={s}')
        for node_id in range(1, n):
            self.exchange(packet('set-id', id=node_id, n=n, peers=ring_peers, build=build), ring_peers[node_id][1:])
        self.table_size, self.hash_table = (s, [[] for _ in range(s)])
        self.exchange(packet('init-table', s=s, build=build))
        for index, row in enumerate(records):
            message = packet('store', row=index, record=row, build=build)
            if int(row[0]) % s % n == 0:
                self.store(message)
            else:
                self.exchange(message)
        counts = self.exchange(packet('count', counts={'0': len(self.saved_rows)}, build=build))['counts']
        if set(counts) != {str(i) for i in range(n)} or sum(counts.values()) != len(records):
            raise ValueError('Count verification failed; completion withheld')
        trace('DHT RECORD COUNTS')
        for node_id, peer in enumerate(ring_peers):
            trace(f'COUNT id={node_id} peer={peer[0]} records={counts[str(node_id)]}')
        trace(f'TOTAL records={sum(counts.values())} expected={len(records)}')
        result = self.manager_request(packet('dht-complete', name=self.name))
        self.finished = result['status'] == 'SUCCESS'
        if self.finished:
            trace('DHT COMPLETE: manager acknowledged SUCCESS')

#-----read terminal commands-----
    def run(self):
        trace('Commands: register NAME IP M_PORT P_PORT | setup-dht NAME N YYYY | status')
        for line in sys.stdin:
            try:
                words = shlex.split(line)
            except ValueError as error:
                trace(f'ERROR: {error}')
                continue
            if not words:
                continue
            try:
                if words[0] == 'register' and len(words) == 5:
                    self.register(*words[1:])
                elif words[0] == 'setup-dht' and len(words) == 4 and self.name:
                    self.setup(*words[1:])
                elif words[0] == 'status' and len(words) == 1:
                    trace(f'STATUS name={self.name} id={self.node_id} s={self.table_size} records={len(self.saved_rows)}')
                elif words[0] == 'dht-complete':
                    trace('dht-complete is sent automatically only after verified distribution')
                else:
                    trace('ERROR: invalid command, arity, or peer not registered')
            except (OSError, ValueError, TimeoutError) as error:
                trace(f'ERROR: {error}')
        threading.Event().wait()
#----start the peer-----
if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.exit('Usage: python3 peer.py MANAGER_IP MANAGER_PORT')
    try:
        Peer((ipv4(sys.argv[1]), port(sys.argv[2]))).run()
    except KeyboardInterrupt:
        pass

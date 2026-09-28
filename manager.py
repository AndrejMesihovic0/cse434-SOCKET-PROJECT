import random
import re
import sys
from collections import OrderedDict
from common import decode, ipv4, port, send, trace, udp_socket

class Manager:

    def __init__(self, server_port):
        self.server_port = server_port
        self.peers = {}     #Stores each peers adress, ports, and current state by name
        self.phase = 'EMPTY'
        self.leader = None
        self.saved_replies = OrderedDict()      #Keeps previous replies so a retried request is not processed twice

    def handle(self, message, source):
        command = message['cmd']
        reply = dict(v=1, cmd='response', tx=message['tx'], status='FAILURE')       #start with failure and change it only after all checks pass.
        try:
            if self.phase == 'BUILDING' and command != 'dht-complete':  #during setup the manager must wait for the leader to finish
                raise ValueError('DHT under construction; only dht-complete accepted')
            name = message['name']
            if command == 'register':
                if not isinstance(name, str) or not re.fullmatch('[A-Za-z]{1,15}', name):
                    raise ValueError('Peer name must contain 1..15 ASCII letters')
                ip = ipv4(message['ip'])
                manager_port, peer_port = (port(message['m_port']), port(message['p_port']))
                used = {self.server_port}       #Include managers port when checking for duplicate ports
                for peer in self.peers.values():
                    used.update((peer['m_port'], peer['p_port']))
                if name in self.peers or manager_port == peer_port or manager_port in used or (peer_port in used):
                    raise ValueError('Duplicate name or port')
                if source != (ip, manager_port):        #The request should come from the adress the peer is registering
                    raise ValueError('Source must match advertised IPv4 address and m-port')
                self.peers[name] = dict(ip=ip, m_port=manager_port, p_port=peer_port, state='Free')
            elif command in ('setup-dht', 'dht-complete'):      #Both commands require a registered peer using its manager port
                peer = self.peers.get(name)
                if peer is None or source != (peer['ip'], peer['m_port']):
                    raise ValueError('Unregistered peer or wrong source endpoint')
                if command == 'setup-dht':
                    n, year = (int(message['n']), int(message['year']))
                    free = [key for key, p in self.peers.items() if p['state'] == 'Free' and key != name]
                    if self.phase != 'EMPTY' or n < 3 or len(free) < n - 1:
                        raise ValueError('DHT exists, n < 3, or insufficient Free peers')
                    if not 1950 <= year <= 2019:
                        raise ValueError('Year must be 1950..2019')
                    selected = [name] + random.sample(free, n - 1)      #Put the leader first, followed by randomly chosen free peers.
                    tuples = [[key, self.peers[key]['ip'], self.peers[key]['p_port']] for key in selected]
                    if len(str(tuples).encode()) > 30000:
                        raise ValueError('Ring metadata too large')
                    for key in selected:
                        self.peers[key]['state'] = 'Leader' if key == name else 'InDHT'
                    self.leader, self.phase = (name, 'BUILDING')
                    reply['peers'] = tuples
                else:           #Only the leader can confirm that construction is complete
                    if name != self.leader or self.phase != 'BUILDING':
                        raise ValueError('Only the current constructing leader may complete')
                    self.phase = 'READY'
            else:
                raise ValueError('Command outside milestone scope')
            reply['status'] = 'SUCCESS'
        except (KeyError, TypeError, ValueError, OverflowError) as error:
            reply['reason'] = str(error)
        return reply

    def run(self):      #listen on all local IPv4 interfaces at the specified port
        sock = udp_socket(('0.0.0.0', self.server_port))
        trace(f'MANAGER listening UDP port={self.server_port}')
        while True:
            data, source = sock.recvfrom(65535)
            try:
                message = decode(data)
                trace(f"RX {message['cmd']} from={source}")
                key = (source, message['tx'])
                if key not in self.saved_replies:
                    self.saved_replies[key] = self.handle(message, source)
                    if len(self.saved_replies) > 4096:
                        self.saved_replies.popitem(last=False)
                reply = self.saved_replies[key]
                send(sock, reply, source)
                trace(f"RESULT {message['cmd']} {reply['status']} phase={self.phase} peers={self.peers}")
            except (ValueError, TypeError, UnicodeError, OSError) as error:
                trace(f'IGNORE malformed datagram: {error}')
if __name__ == '__main__':          # The manager takes one comand line argument Its listening port
    if len(sys.argv) != 2:
        sys.exit('Usage: python3 manager.py PORT')
    try:
        Manager(port(sys.argv[1])).run()
    except KeyboardInterrupt:
        pass

# DHT manager
# run it like: python3 manager.py PORT

import json
import random
import socket
import sys

peers = {}          # info for each peer and what state there in
ring = []           # peers currently in dht, leader comes first
phase = 'NONE'      # current dht phase
changing_peer = None    # whoever started the join/leave


def tuple_of(name):
    # turn peer info into the tuple/list format we send around
    return [name, peers[name]['ip'], peers[name]['p_port']]


def set_ring(names):
    # save new ring and update peer states
    global ring

    # old ring peers go back free first
    for name in ring:
        peers[name]['state'] = 'Free'

    ring = names

    for name in ring:
        # first guy is always leader
        peers[name]['state'] = 'Leader' if name == ring[0] else 'InDHT'


def handle(msg):
    global ring, phase, changing_peer

    cmd, name = msg['cmd'], msg['name']
    peer = peers.get(name)

    # if something is already changing dont allow other stuff
    if phase == 'BUILDING' and cmd != 'dht-complete':
        return {'status': 'FAILURE'}

    if phase == 'REBUILDING' and cmd != 'dht-rebuilt':
        return {'status': 'FAILURE'}

    if phase == 'TEARDOWN' and cmd != 'teardown-complete':
        return {'status': 'FAILURE'}

    # every command besides register has to already be registered
    if cmd != 'register' and peer is None:
        return {'status': 'FAILURE'}

    if cmd == 'register':
        # collect all ports already being used
        used = (
            [p['m_port'] for p in peers.values()]
            + [p['p_port'] for p in peers.values()]
        )

        # basic checks for duplicate names/ports and bad names
        if (
            name in peers
            or not name.isalpha()
            or len(name) > 15
            or msg['m_port'] in used
            or msg['p_port'] in used
        ):
            return {'status': 'FAILURE'}

        peers[name] = {
            'ip': msg['ip'],
            'm_port': msg['m_port'],
            'p_port': msg['p_port'],
            'state': 'Free'
        }

        return {'status': 'SUCCESS'}

    if cmd == 'setup-dht':
        n = msg['n']

        # get free peers but dont include the one asking
        free = [
            p for p in peers
            if peers[p]['state'] == 'Free' and p != name
        ]

        if (
            phase != 'NONE'
            or n < 3
            or len(free) < n - 1
            or peer['state'] != 'Free'
        ):
            return {'status': 'FAILURE'}

        # requester is leader then just pick random other free peers
        set_ring([name] + random.sample(free, n - 1))

        phase = 'BUILDING'

        return {
            'status': 'SUCCESS',
            'peers': [tuple_of(p) for p in ring]
        }

    if cmd == 'dht-complete':
        # only leader should finish setup
        if phase != 'BUILDING' or name != ring[0]:
            return {'status': 'FAILURE'}

        phase = 'READY'
        return {'status': 'SUCCESS'}

    if cmd == 'query-dht':
        # queries come from a free peer
        if phase != 'READY' or peer['state'] != 'Free':
            return {'status': 'FAILURE'}

        # give them a random peer in the ring
        return {
            'status': 'SUCCESS',
            'peer': tuple_of(random.choice(ring))
        }

    if cmd == 'leave-dht':
        # peer gotta actually be in ring to leave it
        if phase != 'READY' or name not in ring:
            return {'status': 'FAILURE'}

        phase, changing_peer = 'REBUILDING', name
        return {'status': 'SUCCESS'}

    if cmd == 'join-dht':
        # only free peer can join
        if phase != 'READY' or peer['state'] != 'Free':
            return {'status': 'FAILURE'}

        phase, changing_peer = 'REBUILDING', name

        # tell joiner who leader is
        return {
            'status': 'SUCCESS',
            'leader': tuple_of(ring[0])
        }

    if cmd == 'dht-rebuilt':
        # only peer that started change can finish it
        if name != changing_peer:
            return {'status': 'FAILURE'}

        # peer sends us the new ring order
        set_ring(msg['ring'])

        phase, changing_peer = 'READY', None
        return {'status': 'SUCCESS'}

    if cmd == 'deregister':
        # cant deregister while your still part of dht
        if peer['state'] != 'Free':
            return {'status': 'FAILURE'}

        del peers[name]
        return {'status': 'SUCCESS'}

    if cmd == 'teardown-dht':
        # leader is only one allowed to teardown
        if phase != 'READY' or name != ring[0]:
            return {'status': 'FAILURE'}

        phase = 'TEARDOWN'
        return {'status': 'SUCCESS'}

    if cmd == 'teardown-complete':
        # make sure teardown was actually started and leader sent it
        if phase != 'TEARDOWN' or name != ring[0]:
            return {'status': 'FAILURE'}

        # everybody in old dht goes back free
        set_ring([])

        phase = 'NONE'
        return {'status': 'SUCCESS'}

    # command wasnt valid
    return {'status': 'FAILURE'}


sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(('', int(sys.argv[1])))

print('Manager listening on port', sys.argv[1])

while True:
    data, addr = sock.recvfrom(65535)

    try:
        msg = json.loads(data)

        print(f"RX {msg['cmd']} from {msg['name']} {addr}")

        reply = handle(msg)

    except Exception:
        # bad msg, just fail instead of manager crashing
        msg, reply = {'name': '?'}, {'status': 'FAILURE'}

    sock.sendto(json.dumps(reply).encode(), addr)

    print(
        f"TX {reply['status']} -> {msg['name']}   "
        f"phase={phase} ring={ring}"
    )

    print(
        '   states:',
        {p: peers[p]['state'] for p in peers}
    )

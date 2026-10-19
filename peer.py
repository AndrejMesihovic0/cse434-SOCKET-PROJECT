# DHT peer thing
# use: python3 peer.py MANAGER_IP MANAGER_PORT

import csv
import json
import os
import queue
import random
import socket
import sys
import threading
import time

FIELDS = ['event_id', 'state', 'year', 'month_name', 'event_type', 'cz_type', 'cz_name', 'injuries_direct',
          'injuries_indirect', 'deaths_direct', 'deaths_indirect', 'damage_property', 'damage_crops', 'tor_f_scale']

manager = (sys.argv[1], int(sys.argv[2]))
name = None
m_sock = None       # manager socket only
p_sock = None       # peer socket, talks to other peers
my_id = None        # my id in the ring leader is 0
ring = []           # all peers in ring order
year = None         # year were using
table = {}          # local table
table_size = None   # size of table
inbox = queue.Queue()  # stuff that comes back to me


def me():
    return [name, p_sock.getsockname()[0], p_sock.getsockname()[1]]


def right():
    # peer thats directly to my right
    return ring[(my_id + 1) % len(ring)]


def send(msg, peer):
    # sends msg to another peer
    p_sock.sendto(json.dumps(msg).encode(), (peer[1], peer[2]))
    if msg['cmd'] != 'store':
        print(f"TX {msg['cmd']} -> {peer[0]}")


def ask_manager(msg):
    # talk to manager and wait for reply
    msg['name'] = name
    m_sock.sendto(json.dumps(msg).encode(), manager)
    print(f"TX {msg['cmd']} -> manager")
    m_sock.settimeout(10)
    reply = json.loads(m_sock.recvfrom(65535)[0])
    print(f"RX manager: {msg['cmd']} {reply['status']}")
    return reply


def wait_for(cmd, timeout=600):
    # waits till the msg im looking for comes back
    while True:
        msg = inbox.get(timeout=timeout)
        if msg['cmd'] == cmd:
            return msg


def set_ring(new_ring, new_id, new_year):
    global ring, my_id, year, table
    ring, my_id, year, table = new_ring, new_id, new_year, {}
    print(f'CONFIG id={my_id} n={len(ring)} right neighbour={right()[0]}')


def leave_ring():
    global ring, my_id, table
    ring, my_id, table = [], None, {}


def count():
    return sum(len(records) for records in table.values())


def read_csv(yyyy):
    # reads the csv and just keeps the fields we actually need
    with open(f'details-{yyyy}.csv', newline='', encoding='utf-8', errors='replace') as f:
        reader = csv.reader(f)
        header = [h.strip().lower() for h in next(reader)]  # first row got column names
        cols = [header.index(field) for field in FIELDS]
        return [[row[c] for c in cols] for row in reader if row]


# -------- dht building stuff, mostly leader does this

def build():
    records = read_csv(year)
    l, n = len(records), len(ring)
    global table_size

    # start above 2*l and keep going till we get a prime
    s = l * 2 + 1
    while any(s % d == 0 for d in range(2, int(s ** 0.5) + 1)):
        s += 1

    table_size = s
    print(f'Read {l} records from details-{year}.csv, table size s={s}')

    for i, record in enumerate(records):
        pos = int(record[0]) % s
        owner = pos % n

        if owner == 0:
            # this ones mine
            table.setdefault(pos, []).append(record)
        else:
            # not mine so send it around ring
            send({'cmd': 'store', 's': s, 'record': record}, right())

            if i % 20 == 0:
                # slow it down a little or udp buffer can get overloaded
                time.sleep(0.01)

        if i % 5000 == 0:
            print(f'  store progress: {i}/{l} records')

    time.sleep(2)

    # go around and get everyones count
    send({'cmd': 'count', 'origin': name, 'counts': [count()]}, right())
    counts = wait_for('count')['counts']

    print('Number of records stored at each peer:')
    for i, c in enumerate(counts):
        print(f'  id {i} ({ring[i][0]}): {c} records')

    print(f'  total {sum(counts)} of {l}')
    return counts


def teardown(keep_ring):
    # clears dht from all peers
    send({'cmd': 'teardown', 'origin': name, 'keep_ring': keep_ring}, right())
    wait_for('teardown')
    print(f'Teardown returned, deleting my hash table ({count()} records)')
    table.clear()


def reset_ids(new_ring, start_id):
    # redo everyones ids after ring changed
    send({'cmd': 'reset-id', 'id': start_id, 'ring': new_ring, 'year': year, 'origin': me()}, new_ring[start_id])
    wait_for('reset-id')
    print(f'reset-id returned: new ring size {len(new_ring)}, leader {new_ring[0][0]}')


def rebuild(msg):
    # leader rebuild the dht after somebody joins/leaves
    if msg['cmd'] == 'join':
        teardown(True)

        # new peer just goes at end
        new_ring = ring + [msg['joiner']]
        set_ring(new_ring, 0, year)
        reset_ids(new_ring, 1)

    counts = build()

    send({
        'cmd': 'rebuild-done',
        'counts': counts,
        'ring': [p[0] for p in ring]
    }, msg['sender'])


# -------- messages coming from other peers

def handle(msg):
    global table_size

    cmd = msg['cmd']

    if cmd == 'set-id':
        # leader telling me my id and ring
        set_ring(msg['ring'], msg['id'], msg['year'])

    elif cmd == 'store':
        table_size = msg['s']
        pos = int(msg['record'][0]) % table_size

        if pos % len(ring) == my_id:
            # belongs here so keep it
            table.setdefault(pos, []).append(msg['record'])
        else:
            # keep passing it till owner gets it
            send(msg, right())

    elif cmd in ('count', 'teardown') and msg['origin'] == name:
        # token made the full circle back to me
        inbox.put(msg)

    elif cmd == 'count':
        msg['counts'].append(count())
        print(f'My count: {count()} records')
        send(msg, right())

    elif cmd == 'teardown':
        print(f'Teardown: deleting my hash table ({count()} records)')

        # save this before leaving ring or right() wont work
        next_peer = right()

        if msg['keep_ring']:
            table.clear()
        else:
            leave_ring()

        send(msg, next_peer)

    elif cmd == 'reset-id':
        if msg['origin'][0] == name:
            # came all the way back already
            inbox.put(msg)
            return

        set_ring(msg['ring'], msg['id'], msg['year'])
        nxt = my_id + 1

        # next peer gets next id, otherwise send back to original
        send(
            dict(msg, id=nxt),
            msg['ring'][nxt] if nxt < len(ring) else msg['origin']
        )

    elif cmd in ('rebuild-dht', 'join'):
        # if i get this then im leader and gotta rebuild
        threading.Thread(target=rebuild, args=(msg,)).start()

    elif cmd == 'find-event':
        find_event(msg)

    else:
        # most likely reply from something i asked for
        inbox.put(msg)


def listen():
    # listens forever for peer messages
    while True:
        data, addr = p_sock.recvfrom(65535)
        msg = json.loads(data)

        if msg['cmd'] != 'store':
            print(f"RX {msg['cmd']} from {addr}")

        try:
            handle(msg)
        except Exception as e:
            print('ERROR handling message:', e)


# -------- hot potato query stuff

def find_event(msg):
    eid = msg['event_id']

    if msg['seq'] == []:
        # first peer sets up who still needs checked
        msg['seq'] = [my_id]
        msg['left'] = [i for i in range(len(ring)) if i != my_id]

    # only check the spot event should hash into
    for record in table.get(eid % table_size, []):
        if int(record[0]) == eid:
            send({
                'cmd': 'find-reply',
                'found': True,
                'record': record,
                'seq': msg['seq']
            }, msg['sender'])
            return

    if not msg['left']:
        # checked everybody and didnt find it
        send({
            'cmd': 'find-reply',
            'found': False,
            'seq': msg['seq']
        }, msg['sender'])
        return

    # choose some random peer that wasnt checked yet
    nxt = random.choice(msg['left'])
    msg['left'].remove(nxt)
    msg['seq'].append(nxt)

    send(msg, ring[nxt])


# -------- commands user types

def do(words):
    global name, m_sock, p_sock, year

    cmd = words[0]

    if cmd == 'register':
        name, ip, m_port, p_port = words[1], words[2], int(words[3]), int(words[4])

        m_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        m_sock.bind((ip, m_port))

        p_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # bigger buffer since stores can come pretty fast
        p_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
        p_sock.bind((ip, p_port))

        if ask_manager({
            'cmd': 'register',
            'ip': ip,
            'm_port': m_port,
            'p_port': p_port
        })['status'] == 'SUCCESS':

            threading.Thread(target=listen, daemon=True).start()

    elif cmd == 'setup-dht':
        n, yyyy = int(words[2]), int(words[3])

        reply = ask_manager({
            'cmd': 'setup-dht',
            'n': n,
            'year': yyyy
        })

        if reply['status'] != 'SUCCESS':
            return

        # im leader so my id starts at 0
        set_ring(reply['peers'], 0, yyyy)

        # tell rest of peers what ids they got
        for i in range(1, n):
            send({
                'cmd': 'set-id',
                'id': i,
                'ring': ring,
                'year': yyyy
            }, ring[i])

        time.sleep(0.5)

        build()
        ask_manager({'cmd': 'dht-complete'})

    elif cmd == 'query-dht':
        reply = ask_manager({'cmd': 'query-dht'})

        if reply['status'] != 'SUCCESS':
            return

        eid = int(words[2])

        # start search from the peer manager gave us
        send({
            'cmd': 'find-event',
            'event_id': eid,
            'sender': me(),
            'seq': [],
            'left': []
        }, reply['peer'])

        answer = wait_for('find-reply', 10)

        if answer['found']:
            for field, value in zip(FIELDS, answer['record']):
                print(f'  {field}: {value}')
        else:
            print(f'Storm event {eid} not found in the DHT.')

        print('  id-seq:', answer['seq'])

    elif cmd == 'leave-dht':
        if ask_manager({'cmd': 'leave-dht'})['status'] != 'SUCCESS':
            return

        # first clear old data
        teardown(True)

        # make new ring starting at my right neighbor, but dont include me
        new_ring = [
            ring[(my_id + 1 + k) % len(ring)]
            for k in range(len(ring) - 1)
        ]

        # right neighbor becomes new leader id 0
        reset_ids(new_ring, 0)

        # tell that new leader to rebuild it
        send({
            'cmd': 'rebuild-dht',
            'sender': me()
        }, new_ring[0])

        done = wait_for('rebuild-done')

        print(
            'Rebuilt DHT counts:',
            dict(zip(done['ring'], done['counts']))
        )

        leave_ring()

        # manager needs new ring info now
        ask_manager({
            'cmd': 'dht-rebuilt',
            'new_leader': new_ring[0][0],
            'ring': done['ring']
        })

    elif cmd == 'join-dht':
        reply = ask_manager({'cmd': 'join-dht'})

        if reply['status'] != 'SUCCESS':
            return

        # ask current leader to put me into ring
        send({
            'cmd': 'join',
            'joiner': me(),
            'sender': me()
        }, reply['leader'])

        done = wait_for('rebuild-done')

        print(
            'Rebuilt DHT counts:',
            dict(zip(done['ring'], done['counts']))
        )

        ask_manager({
            'cmd': 'dht-rebuilt',
            'new_leader': done['ring'][0],
            'ring': done['ring']
        })

    elif cmd == 'teardown-dht':
        if ask_manager({'cmd': 'teardown-dht'})['status'] != 'SUCCESS':
            return

        # completely destroy the ring this time
        teardown(False)
        leave_ring()

        ask_manager({'cmd': 'teardown-complete'})

    elif cmd == 'deregister':
        if ask_manager({'cmd': 'deregister'})['status'] == 'SUCCESS':
            print('Deregistered, exiting')
            os._exit(0)

    elif cmd == 'status':
        print(
            f'name={name} id={my_id} '
            f'ring={[p[0] for p in ring]} records={count()}'
        )

    else:
        print('Unknown command')


print('Commands: register NAME IP M_PORT P_PORT | setup-dht NAME N YYYY | query-dht NAME EVENT_ID')
print('          leave-dht NAME | join-dht NAME | teardown-dht NAME | deregister NAME | status')

for line in sys.stdin:
    if line.split():
        try:
            do(line.split())
        except Exception as e:
            print('ERROR:', e)

threading.Event().wait()

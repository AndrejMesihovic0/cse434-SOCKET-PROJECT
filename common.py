import csv
import ipaddress
import json
import math
import re
import socket
import uuid

#These are the 14 fields we keep from each storm event, in the order the spec lists them
FIELDS = 'event_id state year month_name event_type cz_type cz_name injuries_direct injuries_indirect deaths_direct deaths_indirect damage_property damage_crops tor_f_scale'.split()
MAX_PACKET = 60000  #Biggest message we'll send in one UDP datagram
MIN_PORT = 18000    #Group 34 port range 34/2 * 1000 + 1000 = 18000 and up to 18499
MAX_PORT = 18499

def trace(message):
    print(message, flush=True)      #Flush so the output shows up in order in the terminal

def packet(command, **fields):              #This will build a message as a dictionary
    return dict(v=1, cmd=command, tx=uuid.uuid4().hex, **fields)        #v= protocl version, cmd = command name, tx = random ID so we can match a reply to its request and spot duplicates

def send(sock, message, address):           #Turn the dictionary into JSON bytes and send it over UDP
    data = json.dumps(message, separators=(',', ':')).encode('utf-8')
    if len(data) > MAX_PACKET:
        raise ValueError('Message exceeds supported UDP payload size')
    sock.sendto(data, tuple(address))
    trace(f"TX {message['cmd']} tx={message['tx'][:8]} -> {address}")           #Log every message we send so the demo shows a trace

def reject_json_constant(value):
    raise ValueError(f'Invalid JSON number: {value}')           #json.loads accepts NaN/Infinity by default and we don't want that

def decode(data):           #Turn recieved bytes back into a dictionary and make sure it looks valid
    if len(data) > MAX_PACKET:
        raise ValueError('Message exceeds supported UDP payload size')
    try:
        message = json.loads(data.decode('utf-8'), parse_constant=reject_json_constant)
    except RecursionError as error:         #stops a carash if someone sends super deeply nested json
        raise ValueError('JSON nesting is too deep') from error
    if not isinstance(message, dict) or message.get('v') != 1:
        raise ValueError('Invalid protocol version')
    if not isinstance(message.get('tx'), str) or not re.fullmatch('[0-9a-f]{32}', message['tx']) or (not isinstance(message.get('cmd'), str)):
        raise ValueError('Missing command or transaction ID')
    return message
    
def ipv4(value):            #Checks the string is a real IPv4 adress and errors if not
    return str(ipaddress.IPv4Address(value))

def port(value):            #Makes sure the port is a number inside out groups range
    result = int(value)
    if not MIN_PORT <= result <= MAX_PORT:
        raise ValueError('Group 34 ports must be in 18000..18499')
    return result

def prime_after(value):     #Finds the first prime number bigger than value
    number = max(2, value + 1)
    while any((number % d == 0 for d in range(2, math.isqrt(number) + 1))):
        number += 1
    return number

def dataset(year):              #Each record is a list of the 14 fields and they're all kept as strings
    if not 1950 <= year <= 2019:
        raise ValueError('Year must be 1950..2019')
    records = []
    with open(f'details-{year}.csv', newline='', encoding='utf-8-sig') as stream:
        reader = csv.reader(stream)
        header = next(reader, None)
        if header is None:
            raise ValueError(f'details-{year}.csv is empty')
        names = [x.strip().lower().replace(' ', '_') for x in header]           #Clean up the column names so "EVENT_ID" or "event id" both become "event_id"
        if all((field in names for field in FIELDS)):
            field_positions = [names.index(field) for field in FIELDS]
        elif len(names) == len(FIELDS):
            trace('WARNING: unexpected header names; using the 14 columns in file order')
            field_positions = list(range(len(FIELDS)))
        else:
            raise ValueError('CSV must contain the 14 project fields (event_id ... tor_f_scale)')
        for row in reader:
            if not row or not any((cell.strip() for cell in row)):
                continue
            try:                                            #Pulls out just the 14 fields we need
                record = [row[i] for i in field_positions]
                if int(record[0]) < 0:
                    raise ValueError
            except (IndexError, ValueError) as error:           #Bad row stop before we start building DHT
                raise ValueError(f'Invalid storm record on CSV line {reader.line_num}') from error
            records.append(record)
    if not records:
        raise ValueError(f'details-{year}.csv contains no storm events')
    return records

def udp_socket(address):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)             #Make a UDP socket and bind it to ip,port
    try:
        sock.bind(address)
    except Exception:
        sock.close()            #If binding fails close it so we don't leake it
        raise
    return sock

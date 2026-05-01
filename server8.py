""""
CECS 327 Assignment 8 Server
"""

import socket
import json
import psycopg2
from datetime import datetime, timezone, timedelta

# databases
MY_DB      = "postgresql://neondb_owner:npg_xpCRV7su4oid@ep-raspy-bread-amyiem2n-pooler.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
PARTNER_DB = "postgresql://neondb_owner:npg_oDmwdg7Zb2Nk@ep-icy-mode-am7jaadf-pooler.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

MY_TABLE      = '"IOT Table_virtual"'
PARTNER_TABLE = '"IOT Table_virtual_virtual"'

SERVER_HOST = "0.0.0.0"
SERVER_PORT = 5044 #manually set in google cloud virtual machine
PST         = timezone(timedelta(hours=-8))

# House a metadata
HA_FRIDGE_BOARDS    = ["Fridge Board 8", "Fridge Board duplicate 8"]
HA_DISHWASHER_BOARD = "Dishwasher Board 8"
HA_MOISTURE         = ["Moisture Meter - Moisture Meture", "Moisture Meter Dup 8"]
HA_AMMETER_FRIDGE   = ["Anmeter", "Anmeter 2 Dup 8"]
HA_AMMETER_DISH     = "AnmeterDish"
HA_WATERFLOW        = "WaterFlow"

# House b metadata
HB_FRIDGE_BOARDS    = ["Fridgeboard", "Fridgeboard duplicate"]
HB_DISHWASHER_BOARD = "Washboard"
HB_MOISTURE         = ["Moisture Meter - Moisture1", "Moisture2"]
HB_AMMETER_FRIDGE   = ["Ammeter1", "Ammeter3"]
HB_AMMETER_DISH     = "Ammeter2"
HB_WATERFLOW        = "Float Switch - Float1"


def fetch(db_url, table, since):
    """fetch all rows from a DB/table after a given UTC datetime."""
    conn = psycopg2.connect(db_url)
    with conn.cursor() as cur:
        #SQL query that gets all rows after the given time, newest first.
        # cur.description gives column names, zip pairs each name with its value
        # to build a list of dictionaries
        cur.execute(
            f'SELECT payload::jsonb AS payload, "time" FROM {table} '
            f'WHERE "time" >= %s ORDER BY "time" DESC',
            [since]
        )
        cols = [d[0] for d in cur.description]
        #list of dictionaries where each dictionary represents one row from the database
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    conn.close()
    return rows

#helper method to make sure house b data is fully collected
def get_house_b_rows(since):
    """
    get house b rows for a given time window
    1. check local database for HB data in the window
    2. if no HB data found locally (pre-sharing period), (one day only)
         check partner's DB to fill the gap
    3. merge all the data and remove duplicates with checking timestamp
    """
    # fetch from local DB first
    local_rows = fetch(MY_DB, MY_TABLE, since)
    hb_local   = [r for r in local_rows
                  if r.get("payload", {}).get("board_name") in
                  HB_FRIDGE_BOARDS + [HB_DISHWASHER_BOARD]]

    # since HB has data only in other database, 
    # query partner database directly
    partner_rows = fetch(PARTNER_DB, PARTNER_TABLE, since)

    # Merge by timestamp to avoid duplicates
    seen_ts  = {r.get("time") for r in hb_local}
    combined = list(hb_local)
    for r in partner_rows:
        if r.get("time") not in seen_ts:
            combined.append(r)
            seen_ts.add(r.get("time"))

    source = "local+partner DB" if partner_rows else "local DB only"
    print(f"[Server] House B: {len(hb_local)} local rows, "
          f"{len(partner_rows)} partner rows  {len(combined)} combined ({source})")
    return combined

# linked List data structure
class Node:
    def __init__(self, value, board):
        self.value = value
        self.board = board
        self.next  = None

class LinkedList:
    def __init__(self):
        self.head  = None
        self.count = 0

    def append(self, value, board):
        node = Node(value, board)
        if not self.head:
            self.head = node
        else:
            cur = self.head
            while cur.next:
                cur = cur.next
            cur.next = node
        self.count += 1

    def values(self):
        result, cur = [], self.head
        while cur:
            result.append(cur.value)
            cur = cur.next
        return result

    def average(self):
        vals = self.values()
        return sum(vals) / len(vals) if vals else None



def extract(rows, fields, boards):
    """
    take rows to look for, sensor fields to look for
    and boards to look for, loop through each row in given rows,
    JSON check if the board name matches, if it does, look for sensor
    field in payload, add this value to linked list to do calculations
    on average later
    """
    if isinstance(fields, str): fields = [fields]
    if isinstance(boards, str): boards = [boards]
    ll = LinkedList()
    for row in rows:
        p = row.get("payload") or {}
        if isinstance(p, str):
            try: p = json.loads(p)
            except: continue
        if p.get("board_name") not in boards:
            continue
        for f in fields:
            val = p.get(f)
            if val is not None:
                try: ll.append(float(val), p.get("board_name"))
                except: pass # the data sometimes is buggy so if it isnt just skip
                break
    return ll


#convert time to pst
def to_pst(dt):
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(PST).strftime("%Y-%m-%d %H:%M:%S PST")

# amps *  120 volt(dishwasher/fridge) / 1000 = kWh / 60 because we are generating data once per minute not per hour
def to_kwh(amps):         return (amps * 120) / 1000 / 60 #amperes to kWh converstion rate / 60 because data generated once per minute
def now_utc():            return datetime.now(timezone.utc) #get current time
def to_gallons(L):        return (L * .264) # liters * .264 = gallons approx

#fridge moisture check
def query_moisture():
    now     = now_utc()
    windows = [
        ("Past Hour",  now - timedelta(hours=1)),
        ("Past Week",  now - timedelta(weeks=1)),
        ("Past Month", now - timedelta(days=30)),
    ]
    out = ["QUERY 1: Average Fridge Moisture",
           f"Time (PST): {to_pst(now)}\n"]

    for label, since in windows:
        #we know house a is always in my database
        ha_rows = fetch(MY_DB, MY_TABLE, since)
        ha      = extract(ha_rows, HA_MOISTURE, HA_FRIDGE_BOARDS)

        #house b use helper method to check my local
        #database and then also check partner database
        hb_rows = get_house_b_rows(since)
        hb      = extract(hb_rows, HB_MOISTURE, HB_FRIDGE_BOARDS)

        #combine all the values in the linked lists
        combined = ha.values() + hb.values()
        overall  = sum(combined) / len(combined) if combined else None

        #clean output for demo
        out.append(f"[{label}]")
        if ha.average() is not None:
            out.append("  House A: " + str(round(ha.average(), 2)) + "% RH  (" + str(ha.count) + " readings)")
        else:
            out.append("  House A: No data")
        if hb.average() is not None:
            out.append("  House B: " + str(round(hb.average(), 2)) + "% RH  (" + str(hb.count) + " readings)")
        else:
            out.append("  House B: No data")
        if overall is not None:
            out.append("  Overall: " + str(round(overall, 2)) + "% RH\n")
        else:
            out.append("  Overall: No data\n")


    return "\n".join(out)

#check the average water consumption of each dishwasher and compare
#convert to gallons
def query_water():
    now     = now_utc()
    windows = [
        ("Past Hour",  now - timedelta(hours=1)),
        ("Past Week",  now - timedelta(weeks=1)),
        ("Past Month", now - timedelta(days=30)),
    ]

    out = ["QUERY 2: Average Dishwasher Water Consumption per Cycle",
           f"Time (PST): {to_pst(now)}",
           "Note: Waterflow measured in Liters L\n"]

    for label, since in windows:
        #we know house a is always in my database
        ha_rows = fetch(MY_DB, MY_TABLE, since)
        ha      = extract(ha_rows, HA_WATERFLOW, HA_DISHWASHER_BOARD)

        #house b use helper method to check my local
        #database and then also check partner database
        hb_rows = get_house_b_rows(since)
        hb      = extract(hb_rows, HB_WATERFLOW, HB_DISHWASHER_BOARD)

             #clean output for water usage data
        out.append(f"[{label}]")
        if ha.average() is not None:
            out.append("  House A: " + str(round(ha.average(), 4)) + " L -> "  + str(round(to_gallons(ha.average()), 4)) + " G (gallons) " + str(ha.count) + " cycles)")
        else:
            out.append("  House A: No data")
        if hb.average() is not None:
            out.append("  House B: " + str(round(hb.average(), 4)) + " L -> " +  str(round(to_gallons(hb.average()), 4)) + " G (gallons) " + str(hb.count) + " readings)")
        else:
            out.append("  House B: No data")

       
        out.append("")

    return "\n".join(out)

#compare electricity usage
#this has a problem because our data generated is pretty bad so it sounds
#like our devices are using a bunch of electricity but its just bad data
#like our anmeters are just generating readings between 1-15 A and we just use
#that in the conversion function
def query_electricity():
    now   = now_utc()
    since = now - timedelta(hours=24)

    out = ["QUERY 3: Electricity Comparison in past 24 Hours",
           f"Time (PST): {to_pst(now)}",
           "Formula: kWh = (amps x 120V) / 1000 / 60\n"]

    #house a data is always local
    #collect data from all 3 ammeters and then sum up total
    #electricity used across all appliances
    ha_rows = fetch(MY_DB, MY_TABLE, since)
    ha_f1   = extract(ha_rows, HA_AMMETER_FRIDGE[0], HA_FRIDGE_BOARDS[0])
    ha_f2   = extract(ha_rows, HA_AMMETER_FRIDGE[1], HA_FRIDGE_BOARDS[1])
    ha_d    = extract(ha_rows, HA_AMMETER_DISH,      HA_DISHWASHER_BOARD)
    ha_kwh  = (sum(to_kwh(v) for v in ha_f1.values()) +
               sum(to_kwh(v) for v in ha_f2.values()) +
               sum(to_kwh(v) for v in ha_d.values()))

    #house b get data from both databases
    #for example past hour will be fully in shared data, but the
    #data for the past week/month
    hb_rows = get_house_b_rows(since)
    hb_f1   = extract(hb_rows, HB_AMMETER_FRIDGE[0], HB_FRIDGE_BOARDS[0])
    hb_f2   = extract(hb_rows, HB_AMMETER_FRIDGE[1], HB_FRIDGE_BOARDS[1])
    hb_d    = extract(hb_rows, HB_AMMETER_DISH,      HB_DISHWASHER_BOARD)
    hb_kwh  = (sum(to_kwh(v) for v in hb_f1.values()) +
               sum(to_kwh(v) for v in hb_f2.values()) +
               sum(to_kwh(v) for v in hb_d.values()))

    #output data for each house a device
    out.append("House A breakdown:")
    out.append(f"  Fridge 1 (Fridge Board 8):           {sum(to_kwh(v) for v in ha_f1.values()):.4f} kWh  ({ha_f1.count} readings)")
    out.append(f"  Fridge 2 (Fridge Board duplicate 8): {sum(to_kwh(v) for v in ha_f2.values()):.4f} kWh  ({ha_f2.count} readings)")
    out.append(f"  Dishwasher (Dishwasher Board 8):     {sum(to_kwh(v) for v in ha_d.values()):.4f} kWh  ({ha_d.count} readings)")
    out.append(f"  TOTAL: {ha_kwh:.4f} kWh\n")

    #output data for each house b device
    out.append("House B breakdown:")
    out.append(f"  Fridge 1 (Fridgeboard):              {sum(to_kwh(v) for v in hb_f1.values()):.4f} kWh  ({hb_f1.count} readings)")
    out.append(f"  Fridge 2 (Fridgeboard duplicate):    {sum(to_kwh(v) for v in hb_f2.values()):.4f} kWh  ({hb_f2.count} readings)")
    out.append(f"  Dishwasher (Washboard):              {sum(to_kwh(v) for v in hb_d.values()):.4f} kWh  ({hb_d.count} readings)")
    out.append(f"  TOTAL: {hb_kwh:.4f} kWh\n")

    #present difference in usage to user
    diff = abs(ha_kwh - hb_kwh)
    if ha_kwh > hb_kwh:
        out.append(f"House A consumed MORE electricity by {diff:.4f} kWh.")
    elif hb_kwh > ha_kwh:
        out.append(f"House B consumed MORE electricity by {diff:.4f} kWh.")
    else:
        out.append("Both houses consumed equal electricity.") #this won't happen but just in case

    return "\n".join(out)


#map of all valid questions and what method to call
QUERY_MAP = {
    "What is the average moisture inside our kitchen fridges in the past hour, week and month?":
        query_moisture,
    "What is the average water consumption per cycle across our smart dishwashers in the past hour, week and month?":
        query_water,
    "Which house consumed more electricity in the past 24 hours, and by how much?":
        query_electricity,
}

#use map to handle incoming message
def handle(message):
    handler = QUERY_MAP.get(message.strip())
    if handler:
        return handler()
    return "Sorry, this query cannot be processed. Please try one of the supported queries."

#test

def test_mode():
    """
    test the functions to see if they work
    without having to connect between server and client
    """
    QUERIES = {
        "1": "What is the average moisture inside our kitchen fridges in the past hour, week and month?",
        "2": "What is the average water consumption per cycle across our smart dishwashers in the past hour, week and month?",
        "3": "Which house consumed more electricity in the past 24 hours, and by how much?",
    }
    print("\n" + "=" * 67)
    print("Test mode")
    print("=" * 67)
    for num, q in QUERIES.items():
        print(f"  [{num}] {q}")
    print("  [q]  Exit test mode")
    print("=" * 67)

    while True:
        choice = input("\n  Your choice: ")
        if choice == "q":
            print("\n  Exiting test mode.")
            return
        if choice in QUERIES:
            print("\n  Testing...\n")
            print("=" * 67)
            print(handle(QUERIES[choice]))
            print("=" * 67)
        else:
            print("  Invalid choice. Pick 1, 2, 3 or q.")

def main():
    mode = input("Run in test mode or server mode? t or s")
    if mode == "t":
        test_mode()
        return
    else:
        #set up server
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((SERVER_HOST, SERVER_PORT))
        server.listen(5)
        print(f"[Server] Listening on {SERVER_HOST}:{SERVER_PORT}")

        while True:
            client, addr = server.accept()
            print(f"[Server] Connection from {addr}")
            try:
                while True:
                    data = client.recv(4096)
                    if not data:
                        break
                    message = data.decode("utf-8").strip()
                    print(f"[Server] Query received: {message[:60]}...")
                    response = handle(message)
                    client.send(bytearray(response, encoding="utf-8"))
            except Exception as e:
                print(f"[Server] Error: {e}")
            finally:
                client.close()
                print(f"[Server] Disconnected: {addr}")
                break

if __name__ == "__main__":
    main()

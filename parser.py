import re
from datetime import datetime
import argparse

MAC_MOVE_REGEX = re.compile(
    r"""
    ^(?:                                     # Optional ISO timestamp
        (?P<iso_ts>\d{4}-\d{2}-\d{2}T[^\s]+)\s+
    )?
    (?:
        (?P<device_ip>\d{1,3}(?:\.\d{1,3}){3})\s+:\s+
    )?
    (?P<event_ts>
        \d{4}\s+[A-Za-z]{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}
    )
    (?:\s+[A-Z]+:)?\s+
    (?P<error_type>%L2FM-4-L2FM_MAC_MOVE2):\s+
    Mac\s+(?P<mac>[0-9a-fA-F]{4}\.[0-9a-fA-F]{4}\.[0-9a-fA-F]{4})\s+
    in\s+vlan\s+(?P<vlan>\d+)\s+
    has\s+moved\s+from\s+
    (?P<src>[A-Za-z]+\d+(?:/\d+)*)\s+
    to\s+
    (?P<dst>[A-Za-z]+\d+(?:/\d+)*)
    """,
    re.VERBOSE
)

# def replace_names_from_dict(word, replacemet_dic):
#     """Заменяем названия портов из словаря"""
#     pattern = re.compile('|'.join(re.escape(k) for k in replacemet_dic.keys())) # объединяем замены
#     return pattern.sub(lambda match: replacemet_dic[match.group(0)], word) # меняем

def parse_log_line(line:str):
    """parses log files searching by a pattern
    """    
    line = line.strip()

    match = MAC_MOVE_REGEX.search(line)
    if not match:
        return None

    #error_type, mac, vlan, src, dst, mac, vlan, src, dst = 0,0,0,0,0,0,0,0,0
    

    d = match.groupdict()
    
    #print("d: ", d)
    # for k,v in d.items():
    #     print(f"{k}: {len(v)}")

    try:
        event_ts = datetime.strptime(
            d["event_ts"],
            "%Y %b %d %H:%M:%S"
        )
    except Exception:
        return None
     
    # унифицируем названия портов с таблицей ports в Postgres
    d["src"] = d["src"].replace('Eth', 'Ethernet').replace('Po', 'port-channel')
    
    return {
        "timestamp": event_ts,
        "device_ip": d["device_ip"],
        "error_type": d["error_type"],
        "mac": d["mac"],
        "vlan": int(d["vlan"]),
        "src": d["src"],
        "dst": d["dst"],
        "raw_line": line
    }

def parse_arguments():
    
    parser = argparse.ArgumentParser() 
 
    parser.add_argument("--remote", 
            action="store_true", #default="True",
        help="Use remote paths")  
    
    args = parser.parse_args()
    
    return args
 
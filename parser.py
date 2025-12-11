import re
from datetime import datetime

# MAC MOVE
MOVE_REGEX = re.compile(
    r'^(?P<timestamp>\d{4}\s+\w{3}\s+\s*\d+\s+\d{2}:\d{2}:\d{2})\s+'
    r'(?P<switch_name>\S+)\s+(?P<error_type>%\S+):\s+'
    r'Mac\s+(?P<mac>[0-9a-fA-F\.]+)\s+in\s+vlan\s+(?P<vlan>\d+)\s+has\s+moved\s+from\s+'
    r'(?P<src_if>\S+)\s+to\s+(?P<dst_if>\S+)',
    re.IGNORECASE
)

# FLAP DISABLE
FLAP_REGEX = re.compile(
    r'^(?P<timestamp>\d{4}\s+\w{3}\s+\s*\d+\s+\d{2}:\d{2}:\d{2})\s+'
    r'(?P<switch_name>\S+)\s+(?P<error_type>%\S+):\s+'
    r'Disabling learning in vlan\s+(?P<vlan>\d+)',
    re.IGNORECASE
)

def parse_log_line(line: str, logger=None):
    #line = line.strip()
    if logger:
        logger.info("DEBUG:", repr(line))
        
    match = MOVE_REGEX.match(line.strip()) # поиск сообщения по шаблону
    
    if match:
        dic = match.groupdict()
        dic['timestamp'] = datetime.strptime(dic['timestamp'], "%Y %b %d %H:%M:%S")
        dic["created_at"]= datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        return {
                "timestamp": dic['timestamp'],
                "switch_name": dic["switch_name"],
                "error_type": dic["error_type"],
                "mac": dic["mac"],
                "vlan": int(dic["vlan"]),
                "src": dic["src_if"],
                "dst": dic["dst_if"],
                "raw": line,
                "created_at":dic["created_at"]
                }
    # created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # # Try MOVE
    # m = MOVE_REGEX.match(line)
    # if m:
    #     d = m.groupdict()
    #     ts = datetime.strptime(d["timestamp"], "%Y %b %d %H:%M:%S")
    #     return {
    #         "timestamp": ts,
    #         "switch_name": d["switch_name"],
    #         "error_type": d["error_type"],
    #         "mac": d["mac"],
    #         "vlan": int(d["vlan"]),
    #         "src": d["src_if"],
    #         "dst": d["dst_if"],
    #         "raw": line,
    #         "created_at": created_at
    #     }

    # # Try FLAP
    # m = FLAP_REGEX.match(line)
    # if m:
    #     d = m.groupdict()
    #     ts = datetime.strptime(d["timestamp"], "%Y %b %d %H:%M:%S")
    #     return {
    #         "timestamp": ts,
    #         "switch_name": d["switch_name"],
    #         "error_type": d["error_type"],
    #         "mac": None,
    #         "vlan": int(d["vlan"]),
    #         "src": None,
    #         "dst": None,
    #         "raw": line,
    #         "created_at": created_at
    #     }

    return None

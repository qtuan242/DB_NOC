import os
import paramiko
from typing import List, Tuple
from config import LOG_DIR, DIRPATH, LOG_FILE, DB_FILE
class SFTP_PGWE:
    def __init__(self, host: str, username: str, password: str, port: int = 22):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.transport = None
        self.sftp = None

    def connect(self):
        self.transport = paramiko.Transport((self.host, self.port))
        self.transport.connect(username=self.username, password=self.password)
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)

    def close(self):
        if self.sftp:
            try: self.sftp.close()
            except: pass
        if self.transport:
            try: self.transport.close()
            except: pass

    def read_head2_and_tail(self, remote_path: str, tail_n: int = 4, chunk_size: int = 131072) -> Tuple[str, List[str]]:
        """Đọc line 2 và tail_n dòng cuối qua SFTP."""
        try:
            with self.sftp.open(remote_path, 'rb') as f:
                st = f.stat()
                if st.st_size == 0:
                    return "", []

                _ = f.readline()  # line1
                line2 = f.readline().decode('utf-8', errors='replace').rstrip('\r\n')

                # Tail đọc ngược
                file_size = st.st_size
                pos = file_size
                buf = bytearray()
                nl_count = 0

                while nl_count < tail_n and pos > 0:
                    read_size = min(chunk_size, pos)
                    pos -= read_size
                    f.seek(pos)
                    chunk = f.read(read_size)
                    buf[:0] = chunk
                    nl_count = buf.count(b'\n')

                lines = bytes(buf).splitlines()
                last_lines = [
                    lb.decode('utf-8', errors='replace').rstrip('\r\n')
                    for lb in lines[-tail_n:]
                ]
                return line2, last_lines
        except Exception as e:
            raise RuntimeError(f"SFTP error reading {remote_path}: {e}")

def calculate_kpi_from_lines(
    Node: str, header: str, last_lines: List[str],
    kpi_defs: dict, node_log_dir: str
):
    """Tính KPI và ghi log."""
    if not header or not last_lines:
        print(f"[{Node}] File rỗng hoặc không đọc được")
        return

    os.makedirs(node_log_dir, exist_ok=True)

    data = ''
    header_parts = header.split("|")
    prev_parts = None
    sum_kpi = {k: 0.0 for k in kpi_defs.keys()}
    count_kpi = {k: 0 for k in kpi_defs.keys()}
    last_time_str = ""

    for ln in last_lines:
        parts = ln.split("|")
        if prev_parts is not None:
            time_str = parts[0]
            last_time_str = time_str
            for kpi_name, (completed_counter, attempted_counter) in kpi_defs.items():
                try:
                    idx_completed = header_parts.index(completed_counter)
                    idx_attempted = header_parts.index(attempted_counter)

                    completed_prev = int(prev_parts[idx_completed])
                    completed_curr = int(parts[idx_completed])
                    attempted_prev = int(prev_parts[idx_attempted])
                    attempted_curr = int(parts[idx_attempted])

                    attempted_diff = attempted_curr - attempted_prev
                    completed_diff = completed_curr - completed_prev

                    if attempted_diff > 0:
                        ratio = completed_diff * 100.0 / attempted_diff
                    else:
                        ratio = 0.0

                    sum_kpi[kpi_name] += ratio
                    count_kpi[kpi_name] += 1

                    data += f"{Node};{time_str};{kpi_name};{ratio:.2f}\n"
                except ValueError:
                    pass
        prev_parts = parts

    # Thêm dòng avg cuối
    if last_time_str:
        for kpi_name in kpi_defs.keys():
            if count_kpi[kpi_name] > 0:
                avg_ratio = sum_kpi[kpi_name] / count_kpi[kpi_name]
                data += f"insertDB;{Node};{last_time_str};{kpi_name};{avg_ratio:.2f}\n"

    file_path = os.path.join(node_log_dir, f"log_{Node}.txt")
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(data)
    # print(data)

def KPI_PGW(node: str, ip: str, user: str, password: str, filename: str, node_log_dir: str):
    """Worker PGW."""
    kpi_defs = {
        "PgwS5CreateSessionFR": (
            "pgw-completed-eps-bearer-stats:pgw-completed-eps-bearer-activation",
            "pgw-attempted-eps-bearer-stats:pgw-attempted-eps-bearer-activation"
        ),
        "SgwS4S11CreateSessionFR": (
            "sgw-gtp-tunnel-mgmt-s4-s11:sm-create-session-resp-acc-sent",
            "sgw-gtp-tunnel-mgmt-s4-s11:sm-create-session-req-rcvd"
        ),
        "GgsnCreatePdpCtxFR": (
            "ggsn-pdp-contexts-stats-completed:ggsn-completed-activation",
            "ggsn-pdp-contexts-stats-attempted:ggsn-attempted-activation"
        ),
    }

    client = SFTP_PGWE(ip, user, password)
    try:
        client.connect()
        header, last_lines = client.read_head2_and_tail(filename, tail_n=4)
    finally:
        client.close()

    calculate_kpi_from_lines(node, header, last_lines, kpi_defs, node_log_dir)
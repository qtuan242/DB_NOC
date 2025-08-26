import os
import re
import paramiko
import logging
from datetime import datetime
from stat import S_ISREG

from config import LOG_DIR, DIRPATH, SFTP_CMD_NOPASS

class Kpi_SBG:
    def __init__(self, node: str, ip: str, user: str, password: str, port: int = 22, type_filter: str = "SBG"):
        self.node = node
        self.ip = ip
        self.user = user
        self.password = password
        self.port = port
        self.type_filter = type_filter
        self.client: paramiko.SSHClient | None = None
        self.sftp: paramiko.SFTPClient | None = None
        self.logger = self._init_logger()

    # ----------------- Logging -----------------
    def _init_logger(self):
        logger = logging.getLogger(f"{self.type_filter}_{self.node}")
        logger.setLevel(logging.INFO)

        # log rotate (per type)
        log_path = os.path.join(LOG_DIR, f"{self.type_filter.lower()}_schedule.log")
        fh = logging.FileHandler(log_path, mode="a", encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

        # overwrite last job
        last_path = os.path.join(LOG_DIR, "last_job.log")
        fh_last = logging.FileHandler(last_path, mode="w", encoding="utf-8")
        fh_last.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

        # tránh add handler nhiều lần khi gọi nhiều job
        if not logger.handlers:
            logger.addHandler(fh)
            logger.addHandler(fh_last)
        return logger

    # ----------------- SSH/SFTP -----------------
    def connect(self):
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(self.ip, self.port, self.user, self.password)
        self.sftp = self._open_sftp_via_sudo_su()
        self.logger.info(f"Connected to {self.node} ({self.ip})")

    def _sftp_from_channel_compat(self, chan: paramiko.Channel) -> paramiko.SFTPClient:
        try:
            return paramiko.SFTPClient.from_channel(chan)
        except AttributeError:
            return paramiko.SFTPClient(chan)

    def _open_sftp_via_sudo_su(self, timeout: int = 30) -> paramiko.SFTPClient:
        transport = self.client.get_transport()
        ch = transport.open_session(timeout=timeout)
        ch.exec_command(SFTP_CMD_NOPASS)
        return self._sftp_from_channel_compat(ch)

    def close(self):
        if self.sftp:
            self.sftp.close()
        if self.client:
            self.client.close()
        self.logger.info(f"Closed connection {self.node}")

    # ----------------- File & Data -----------------
    def _newest_file_in_dir(self, dirpath: str):
        latest_attr = None
        latest_name = None
        for attr in self.sftp.listdir_attr(dirpath):
            if not S_ISREG(attr.st_mode):
                continue
            if (latest_attr is None
                or attr.st_mtime > latest_attr.st_mtime
                or (attr.st_mtime == latest_attr.st_mtime and attr.st_size > latest_attr.st_size)):
                latest_attr = attr
                latest_name = attr.filename
        if latest_attr is None:
            return None, None
        return f"{dirpath.rstrip('/')}/{latest_name}", latest_attr

    def _read_from_last_header_to_eof_sftp(
        self,
        filepath: str,
        header_line: str,
        chunk_size: int = 65536,
        max_scan_bytes: int | None = 16 * 1024 * 1024,
        encoding: str = "utf-8",
    ) -> str:
        needle = header_line.encode(encoding, errors="ignore")
        with self.sftp.open(filepath, "rb") as f:
            st = f.stat()
            if st.st_size == 0:
                return ""

            size = st.st_size
            pos = size
            scanned = 0
            acc = bytearray()

            while pos > 0 and (max_scan_bytes is None or scanned < max_scan_bytes):
                rd = min(chunk_size, pos)
                pos -= rd
                f.seek(pos)
                block = f.read(rd)
                scanned += rd
                acc[:0] = block

                idx = acc.rfind(needle)
                if idx != -1:
                    header_pos = pos + idx
                    f.seek(header_pos)
                    tail = f.read(size - header_pos)
                    return tail.decode(encoding, errors="replace")
            return ""

    def _average(self, seq):
        seq = [x for x in seq if x is not None]
        return (sum(seq) / len(seq)) if seq else 0.0

    def _analyze_kpi_lines(self, lines, current_time, node_log_dir, skip_if_all_ratios_zero=True):
        subIPv4 = subIPv6 = sub = 0
        InitRegTimeIPv4 = InitRegTimeIPv6 = InitRegTime_ = 0
        succRegisIPv4, IncSessionRateIv4, OutSessionRateIv4 = [], [], []
        succRegisIPv6, IncSessionRateIv6, OutSessionRateIv6 = [], [], []
        succRegis, IncSessionRate, OutSessionRate = [], [], []

        for line in lines:
            fields = [c.strip() for c in line.split(',')]
            if len(fields) < 15:
                continue
            try:
                ipVersion = fields[4]
                numSub   = int(float(fields[5] or 0))
                InitRegTime = float(fields[7] or 0)
                ratio    = float(fields[6]  or 0)
                incRatio = float(fields[13] or 0)
                outRatio = float(fields[14] or 0)

                if numSub == 0:
                    continue
                if skip_if_all_ratios_zero and (ratio == 0 and incRatio == 0 and outRatio == 0):
                    continue

                if ipVersion == 'IPv4':
                    InitRegTimeIPv4 += InitRegTime
                    subIPv4 += numSub
                    succRegisIPv4.append(ratio)
                    IncSessionRateIv4.append(incRatio)
                    OutSessionRateIv4.append(outRatio)
                elif ipVersion == 'IPv6':
                    InitRegTimeIPv6 += InitRegTime
                    subIPv6 += numSub
                    succRegisIPv6.append(ratio)
                    IncSessionRateIv6.append(incRatio)
                    OutSessionRateIv6.append(outRatio)

                InitRegTime_ += InitRegTime
                sub += numSub
                succRegis.append(ratio)
                IncSessionRate.append(incRatio)
                OutSessionRate.append(outRatio)
            except Exception as e:
                self.logger.warning(f"Error processing line: {line} => {e}")

        data = ""
        if subIPv4 and InitRegTimeIPv4:
            data += f"insertDB;{self.node};{current_time};subIPv4;{subIPv4}\n"
            data += f"insertDB;{self.node};{current_time};subIPv6;{subIPv6}\n"
            data += f"insertDB;{self.node};{current_time};InitRegTimeIPv4;{InitRegTimeIPv4:.0f}\n"
            data += f"insertDB;{self.node};{current_time};InitRegTimeIPv6;{InitRegTimeIPv6:.0f}\n"
            data += f"insertDB;{self.node};{current_time};RegRatioV4;{self._average(succRegisIPv4):.2f}\n"
            data += f"insertDB;{self.node};{current_time};RegRatioV6;{self._average(succRegisIPv6):.2f}\n"
            data += f"insertDB;{self.node};{current_time};IncSessionRateIv4;{self._average(IncSessionRateIv4):.2f}\n"
            data += f"insertDB;{self.node};{current_time};IncSessionRateIv6;{self._average(IncSessionRateIv6):.2f}\n"
            data += f"insertDB;{self.node};{current_time};OutSessionRateIv4;{self._average(OutSessionRateIv4):.2f}\n"
            data += f"insertDB;{self.node};{current_time};OutSessionRateIv6;{self._average(OutSessionRateIv6):.2f}\n"
            data += f"insertDB;{self.node};{current_time};sub;{sub}\n"
            data += f"insertDB;{self.node};{current_time};InitRegTime_;{InitRegTime_:.0f}\n"
            data += f"insertDB;{self.node};{current_time};succRegis;{self._average(succRegis):.2f}\n"
            data += f"insertDB;{self.node};{current_time};IncSessionRate;{self._average(IncSessionRate):.2f}\n"
            data += f"insertDB;{self.node};{current_time};OutSessionRate;{self._average(OutSessionRate):.2f}\n"

        file_path = os.path.join(node_log_dir, f"log_{self.node}.txt")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(data)

    # ----------------- Public API -----------------
    def run(self, node_log_dir: str):
        try:
            self.connect()
            fullpath, attr = self._newest_file_in_dir(DIRPATH)
            if not fullpath:
                self.logger.warning(f"No file found in {DIRPATH}")
                return
            block = self._read_from_last_header_to_eof_sftp(
                fullpath,
                header_line="Timestamp,PmpId,CpuLoadCh,CpuLoadSb,MemoryLoadCh,MemoryLoadSb,CpRegUsers,CpSessions",
            )
            if not block:
                self.logger.warning("No header found in scanned region")
                return

            lines = [line.strip() for line in block.split("\n") if 'IPv' in line and 'access' in line]
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M")
            self._analyze_kpi_lines(lines, current_time, node_log_dir)
            self.logger.info(f"Processed KPI for {self.node}, lines={len(lines)}")
        finally:
            self.close()




def Kpi_SBG_run(node, ip, user, password, node_log_dir, port=22):
    worker = Kpi_SBG(node=node, ip=ip, user=user, password=password, port=port, type_filter="SBG")
    worker.run(node_log_dir)
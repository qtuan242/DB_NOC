# ssh_module.py

import time
from time import sleep

import paramiko
import os
import datetime
class SSH:
    def __init__(self, ip, username, password, prompt='#', timeout=5):
        self.prompt = prompt  # 👈 thêm dòng này
        self._client = paramiko.SSHClient()
        self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._client.connect(
            hostname=ip,
            username=username,
            password=password,
            look_for_keys=False,
            allow_agent=False
        )
        self._ssh = self._client.invoke_shell()
        self._wait_for_prompt(prompt=prompt, timeout=timeout)

    def _wait_for_prompt(self, prompt='#', timeout=7):
        """Đợi đến khi thiết bị in ra dấu prompt (vd: #, >, $, ...)"""
        buffer = ''
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self._ssh.recv_ready():
                recv = self._ssh.recv(1024).decode('utf-8')
                buffer += recv
                if prompt in buffer:
                    break
            time.sleep(0.01)
        return buffer.strip()  # 👈 có thể trả lại nội dung nếu muốn dùng

    def send_show_command(self, command, timeout=5):
        """Gửi lệnh và chờ đến khi prompt xuất hiện trở lại"""
        self._ssh.send(command + '\n')
        time.sleep(0.01)
        output = self._wait_for_prompt(prompt=self.prompt, timeout=timeout)
        return output

    def close_connection(self):
        self._client.close()



def Kpi_MME(name, ip, username, password, kpiList, log_filepath):
    # kpiList = ['attach_wcdma', 'pdp_activation_wcdma','paging_wcdma', 'attach_lte', 'paging_lte', 'bearer_establishment_lte']
    ssh = SSH(ip, username, password, prompt='#')
    data = ''
    datainsert = ''
    cmd = "pdc_kpi.pl -i 3 -l"
    output = ssh.send_show_command(cmd)
    data += output
    for line in output.splitlines():
        line = line.strip()
        if line.lower().startswith("day"):
            DateData = line.split(":")[1].strip()
            today = datetime.date.today()
            year = today.year
            month = today.month
            day = int(DateData)
            fullDate = f"{year}-{month:02d}-{day:02d}"
        elif line.lower().startswith("time"):
            time_str = line.split(":", 1)[1].strip()

        for kpi in kpiList:
            if line.lower().startswith(kpi):
                KpiName, KpiValue = line.strip().split(":", 1)
                KpiName = KpiName.strip()
                KpiValue = 100 - float(KpiValue.strip().rstrip("%"))
                datainsert +=f"\ninsertDB;{name};{fullDate} {time_str};{KpiName};{KpiValue}\n"
    output = ssh.send_show_command("pdc_kpi.pl -q 1,5 -i 3 | grep %")
    data += output
    lines = output.strip().splitlines()
    qcilog = ""

    i = 1
    for idx, line in enumerate(lines):
        if "%" in line:
            parts = line.split()
            if len(parts) >= 5:
                qcilog += f"\n{i}-{parts[3]}-{parts[4]}-{line}"
                if idx == 2:  # dòng thứ 2 (index 1)
                    qci1 = parts[4].replace('%', '')
                    qci1 = 100 - float(qci1)

                if idx == 3:  # dòng thứ 3 (index 2)
                    qci5 = parts[3].replace('%', '')
                    qci5 = 100 - float(qci5)

                i += 1
    if qci1 and qci5:
            datainsert += f"\ninsertDB;{name};{fullDate} {time_str};qci1;{qci1}\n"
            datainsert += f"\ninsertDB;{name};{fullDate} {time_str};qci5;{qci5}\n"
    data +=datainsert
    # print(data)
    # 🔻 Tạo folder nếu chưa có
    # log_dir = f"logs/mme"
    # os.makedirs(log_dir, exist_ok=True)
    #
    # # 🔻 Ghi log theo tên node
    # file_path = os.path.join(log_dir, f"log_{name}.txt")
    with open(log_filepath, "w", encoding="utf-8") as f:
        f.write(data)
    ssh.close_connection()




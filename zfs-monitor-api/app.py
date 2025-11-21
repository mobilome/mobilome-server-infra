#!/usr/bin/env python3
from flask import Flask, jsonify
from flask_cors import CORS
import paramiko
import concurrent.futures
import json

# ------------------------------
# 读取配置文件
# ------------------------------
with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

ZFS_SERVERS = config.get("zfs_servers", [])

# ------------------------------
# SSH 远程执行命令函数
# ------------------------------

def ssh_run(server):
    host = server["host"]
    user = server["user"]
    password = server["password"]
    dataset = server["dataset"]

    cmd = f"zfs userspace -H -o name,used,quota,objused,objquota -S used {dataset}"

    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        ssh.connect(host, username=user, password=password, timeout=5)
        stdin, stdout, stderr = ssh.exec_command(cmd, timeout=5)

        output = stdout.read().decode()
        err = stderr.read().decode()

        ssh.close()

        # zfs 输出错误也要返回正常结构，只是 users 空 + error 字段
        if err.strip():
            return {
                "server": server["name"],
                "host": host,
                "dataset": dataset,
                "users": [],
                "error": err.strip()
            }

        users = []
        for line in output.strip().splitlines():
            parts = line.split("\t")
            if len(parts) == 5:
                users.append({
                    "user": parts[0],
                    "used": parts[1],
                    "quota": parts[2],
                    "objused": parts[3],
                    "objquota": parts[4]
                })

        return {
            "server": server["name"],
            "host": host,
            "dataset": dataset,
            "users": users
        }

    except Exception as e:
        # 任何错误都返回同结构 + 空 users
        return {
            "server": server["name"],
            "host": host,
            "dataset": dataset,
            "users": [],
            "error": str(e)
        }


# ------------------------------
# Flask API
# ------------------------------
app = Flask(__name__)
CORS(app)

@app.route("/user_disk_usage")
def multi_usage():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(ssh_run, ZFS_SERVERS))
    return jsonify(results)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=19998)

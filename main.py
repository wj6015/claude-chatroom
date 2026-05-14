#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
import math
import queue
import logging
import threading
import subprocess
import urllib.request
from datetime import datetime
from collections import Counter

import tkinter as tk
from tkinter import messagebox, scrolledtext

import pandas as pd

from websocket import create_connection


# =========================================================
# 基础配置
# =========================================================

APP_TITLE = "收费情况明细采集工具_纯CDP版"

BASE_DIR = os.path.dirname(os.path.abspath(sys.argv[0]))

SG_BROWSER_PATH = r"C:\Users\Administrator\AppData\Local\EPRI\SGSBrowser\Application\sgsbrowser.exe"

SG_USER_DATA_DIR = os.path.join(BASE_DIR, "sgs_cdp_user_data")

DEBUG_HOST = "127.0.0.1"
DEBUG_PORT = 9222

VERSION_URL = f"http://{DEBUG_HOST}:{DEBUG_PORT}/json/version"
TABS_URL = f"http://{DEBUG_HOST}:{DEBUG_PORT}/json"

TARGET_API_KEYWORD = "queryTransPqList"

PAGE_SIZE = 100

NETWORK_TIMEOUT = 90

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


# =========================================================
# 字段
# =========================================================

FIELD_MAP = [
    ("sgsMgtOrgName", "市公司"),
    ("xgsMgtOrgName", "县公司"),
    ("gdsMgtOrgName", "供电所"),
    ("expYm", "电费年月"),
    ("expYmd", "现货日"),
    ("deregMemberId", "市场主体标识"),
    ("planNo", "计划编号"),
    ("custNo", "用户编号"),
    ("custName", "用户名称"),
    ("pq2401", "01:00电量"),
    ("pq2402", "02:00电量"),
    ("pq2403", "03:00电量"),
    ("pq2404", "04:00电量"),
    ("pq2405", "05:00电量"),
    ("pq2406", "06:00电量"),
    ("pq2407", "07:00电量"),
    ("pq2408", "08:00电量"),
    ("pq2409", "09:00电量"),
    ("pq2410", "10:00电量"),
    ("pq2411", "11:00电量"),
    ("pq2412", "12:00电量"),
    ("pq2413", "13:00电量"),
    ("pq2414", "14:00电量"),
    ("pq2415", "15:00电量"),
    ("pq2416", "16:00电量"),
    ("pq2417", "17:00电量"),
    ("pq2418", "18:00电量"),
    ("pq2419", "19:00电量"),
    ("pq2420", "20:00电量"),
    ("pq2421", "21:00电量"),
    ("pq2422", "22:00电量"),
    ("pq2423", "23:00电量"),
    ("pq2424", "24:00电量"),
    ("yearAvgPq", "本年日均电量"),
    ("dayPq", "总电量"),
    ("mtmUp", "环比"),
    ("monAvgPq", "上月日均电量"),
    ("ytyUp", "同比"),
]

EXPORT_COLUMNS = [x[1] for x in FIELD_MAP] + ["重复标志"]


# =========================================================
# 工具
# =========================================================

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def http_get_json(url):
    with urllib.request.urlopen(url, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))


def port_ready():
    try:
        http_get_json(VERSION_URL)
        return True
    except Exception:
        return False


# =========================================================
# CDP Client
# =========================================================

class CDPClient:

    def __init__(self, ws_url):

        self.ws = create_connection(ws_url)

        self.msg_id = 0

        self.pending = {}

        self.running = True

        self.network_queue = queue.Queue()

        self.recv_thread = threading.Thread(
            target=self.recv_loop,
            daemon=True
        )

        self.recv_thread.start()

        self.send("Network.enable", {})
        self.send("Page.enable", {})
        self.send("Runtime.enable", {})

    def next_id(self):
        self.msg_id += 1
        return self.msg_id

    def send(self, method, params=None):

        if params is None:
            params = {}

        mid = self.next_id()

        payload = {
            "id": mid,
            "method": method,
            "params": params
        }

        self.ws.send(json.dumps(payload))

        return mid

    def call(self, method, params=None, timeout=20):

        mid = self.send(method, params)

        deadline = time.time() + timeout

        while time.time() < deadline:

            if mid in self.pending:
                return self.pending.pop(mid)

            time.sleep(0.02)

        raise TimeoutError(method)

    def recv_loop(self):

        while self.running:

            try:
                raw = self.ws.recv()

                msg = json.loads(raw)

                if "id" in msg:
                    self.pending[msg["id"]] = msg

                else:
                    self.handle_event(msg)

            except Exception:
                pass

    def handle_event(self, msg):

        method = msg.get("method")

        if method == "Network.responseReceived":

            params = msg.get("params", {})

            response = params.get("response", {})

            url = response.get("url", "")

            if TARGET_API_KEYWORD in url:

                self.network_queue.put(params)

    def eval(self, js):

        result = self.call(
            "Runtime.evaluate",
            {
                "expression": js,
                "returnByValue": True
            }
        )

        return result["result"]["result"].get("value")

    def get_response_body(self, request_id):

        result = self.call(
            "Network.getResponseBody",
            {
                "requestId": request_id
            }
        )

        return result["result"]["body"]


# =========================================================
# Collector
# =========================================================

class Collector:

    def __init__(self, gui):

        self.gui = gui

        self.cdp = None

        self.page_rows = {}

        self.stop_flag = False

    def log(self, s):

        self.gui.log_queue.put(s + "\n")

    # -----------------------------------------------------

    def launch_browser(self):

        if port_ready():
            return

        ensure_dir(SG_USER_DATA_DIR)

        args = [
            SG_BROWSER_PATH,
            f"--remote-debugging-port={DEBUG_PORT}",
            f"--user-data-dir={SG_USER_DATA_DIR}",
            "--no-first-run",
            "--no-default-browser-check",
        ]

        subprocess.Popen(
            args,
            cwd=os.path.dirname(SG_BROWSER_PATH)
        )

        deadline = time.time() + 20

        while time.time() < deadline:

            if port_ready():
                return

            time.sleep(0.5)

        raise RuntimeError("9222端口启动失败")

    # -----------------------------------------------------

    def connect(self):

        self.launch_browser()

        tabs = http_get_json(TABS_URL)

        if not tabs:
            raise RuntimeError("未找到浏览器tab")

        tab = tabs[0]

        ws_url = tab["webSocketDebuggerUrl"]

        self.cdp = CDPClient(ws_url)

    # -----------------------------------------------------

    def get_total(self):

        js = r"""
(function(){

let el=document.querySelector('.el-pagination__total');

if(!el)return 0;

let m=el.innerText.match(/\d+/);

if(!m)return 0;

return parseInt(m[0]);

})();
"""

        return self.cdp.eval(js)

    # -----------------------------------------------------

    def goto_page(self, page):

        js = f"""
(function(){{

let input=document.querySelector('.el-pagination__jump input');

if(!input)return false;

input.focus();

input.value='{page}';

input.dispatchEvent(new Event('input', {{ bubbles:true }}));

input.dispatchEvent(new Event('change', {{ bubbles:true }}));

input.dispatchEvent(new KeyboardEvent(
'keydown',
{{
key:'Enter',
code:'Enter',
keyCode:13,
which:13,
bubbles:true
}}
));

return true;

}})();
"""

        return self.cdp.eval(js)

    # -----------------------------------------------------

    def wait_payload(self):

        deadline = time.time() + NETWORK_TIMEOUT

        while time.time() < deadline:

            try:

                params = self.cdp.network_queue.get(timeout=1)

                rid = params["requestId"]

                time.sleep(1.2)

                body = self.cdp.get_response_body(rid)

                return json.loads(body)

            except Exception:
                pass

        return None

    # -----------------------------------------------------

    def payload_rows(self, payload):

        data = payload.get("data")

        if isinstance(data, dict):

            rows = data.get("list", [])

        else:
            rows = []

        out = []

        for item in rows:

            rec = {}

            for raw, cn in FIELD_MAP:
                rec[cn] = item.get(raw, "")

            out.append(rec)

        return out

    # -----------------------------------------------------

    def make_repeat_marks(self, rows):

        def k(r):
            return tuple(r.get(cn, "") for _, cn in FIELD_MAP)

        cnt = Counter(k(x) for x in rows)

        gid = {}

        g = 1

        result = []

        for r in rows:

            key = k(r)

            mark = ""

            if cnt[key] > 1:

                if key not in gid:
                    gid[key] = str(g)
                    g += 1

                mark = gid[key]

            arr = [r.get(cn, "") for _, cn in FIELD_MAP]

            arr.append(mark)

            result.append(arr)

        return result

    # -----------------------------------------------------

    def export_excel(self):

        all_rows = []

        for p in sorted(self.page_rows.keys()):
            all_rows.extend(self.page_rows[p])

        rows = self.make_repeat_marks(all_rows)

        df = pd.DataFrame(
            rows,
            columns=EXPORT_COLUMNS
        )

        out = os.path.join(
            BASE_DIR,
            f"采集结果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        )

        df.to_excel(out, index=False)

        self.log(f"导出成功: {out}")

    # -----------------------------------------------------

    def collect(self):

        try:

            self.log("等待第一页数据...")

            payload = self.wait_payload()

            if not payload:
                self.log("未获取第一页数据")
                return

            total = payload["data"]["total"]

            max_pages = math.ceil(total / PAGE_SIZE)

            rows = self.payload_rows(payload)

            self.page_rows[1] = rows

            self.log(f"总条数: {total}")
            self.log(f"总页数: {max_pages}")

            for page in range(2, max_pages + 1):

                if self.stop_flag:
                    break

                self.log(f"跳转第 {page} 页")

                self.goto_page(page)

                payload = self.wait_payload()

                if not payload:
                    self.log(f"第 {page} 页超时")
                    continue

                rows = self.payload_rows(payload)

                self.page_rows[page] = rows

                total_rows = sum(
                    len(v)
                    for v in self.page_rows.values()
                )

                self.log(
                    f"[P{page}] {len(rows)} 条 "
                    f"累计 {total_rows}"
                )

            self.export_excel()

            self.log("采集完成")

        except Exception as e:

            self.log(str(e))


# =========================================================
# GUI
# =========================================================

class GUI:

    def __init__(self, root):

        self.root = root

        self.root.title(APP_TITLE)

        self.root.geometry("980x720")

        self.log_queue = queue.Queue()

        self.collector = Collector(self)

        top = tk.Frame(root)
        top.pack(pady=10)

        tk.Button(
            top,
            text="1.启动国网浏览器",
            width=20,
            command=self.open_browser
        ).grid(row=0, column=0, padx=5)

        tk.Button(
            top,
            text="2.开始采集",
            width=20,
            command=self.start_collect
        ).grid(row=0, column=1, padx=5)

        self.text = scrolledtext.ScrolledText(
            root,
            width=130,
            height=38
        )

        self.text.pack()

        self.root.after(100, self.process_queue)

    def process_queue(self):

        try:

            while True:

                msg = self.log_queue.get_nowait()

                self.text.insert(tk.END, msg)

                self.text.see(tk.END)

        except queue.Empty:
            pass

        self.root.after(100, self.process_queue)

    def open_browser(self):

        try:

            self.collector.connect()

            messagebox.showinfo(
                "成功",
                "浏览器已连接\n请手工打开目标页面并查询第一页"
            )

        except Exception as e:

            messagebox.showerror(
                "错误",
                str(e)
            )

    def start_collect(self):

        threading.Thread(
            target=self.collector.collect,
            daemon=True
        ).start()


# =========================================================

if __name__ == "__main__":

    root = tk.Tk()

    GUI(root)

    root.mainloop()

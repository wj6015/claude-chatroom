#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import time
import math
import json
import threading
import logging
import queue
import subprocess
import urllib.request
from datetime import datetime
from decimal import Decimal, InvalidOperation
from collections import Counter

import pandas as pd
import tkinter as tk
from tkinter import messagebox, scrolledtext

from websocket import create_connection


# ================= 基础配置 =================
APP_TITLE = "Web列表采集工具_国网浏览器CDP版"

BASE_DIR = os.path.dirname(os.path.abspath(sys.argv[0]))

SG_BROWSER_PATH = r"C:\Users\Administrator\AppData\Local\EPRI\SGSBrowser\Application\sgsbrowser.exe"
SG_USER_DATA_DIR = os.path.join(BASE_DIR, "sgs_cdp_user_data")

DEBUG_HOST = "127.0.0.1"
DEBUG_PORT = 9222
DEBUG_VERSION_URL = f"http://{DEBUG_HOST}:{DEBUG_PORT}/json/version"
DEBUG_TABS_URL = f"http://{DEBUG_HOST}:{DEBUG_PORT}/json"

TARGET_API_KEYWORD = "queryTransPqList"
PAGE_SIZE = 100
NETWORK_RESPONSE_TIMEOUT = 60  # 页面显示快，但 Preview.data 可能延迟，适度放宽
PAGE_CHANGE_TIMEOUT = 15
PAGE_SETTLE_DELAY = 0.5  # 页码高亮后再给接口一点时间完成刷新

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")


# ================= 字段映射（38个） =================
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

EXPORT_COLUMNS = [cn for _, cn in FIELD_MAP] + ["重复标志"]


def clean_cell(v):
    if v is None:
        return ""
    if isinstance(v, str):
        s = v.strip()
        return "" if s.lower() in ("none", "null", "nan") else s
    return v


def normalize_text(v):
    if v is None:
        return ""
    s = str(v).strip()
    if s.lower() in ("none", "null", "nan"):
        return ""
    return s


def normalize_amount(v):
    s = normalize_text(v)
    if s == "":
        return ""
    s = s.replace(",", "")
    try:
        d = Decimal(s)
        s2 = format(d.normalize(), "f")
        if "." in s2:
            s2 = s2.rstrip("0").rstrip(".")
        return s2
    except (InvalidOperation, ValueError):
        return s


def _ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def _http_get_json(url: str, timeout: float = 3.0):
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
        return json.loads(raw)


def _port_ready(url: str, timeout: float = 1.2) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return resp.status == 200
    except Exception:
        return False


# ================= 纯 CDP 客户端 =================
class CDPClient:
    """
    纯 WebSocket CDP 客户端。
    目的：替代 Selenium/chromedriver，但保留原来的采集流程和等待逻辑。
    """

    def __init__(self, ws_url, log_func=None):
        self.ws_url = ws_url
        self.ws = create_connection(ws_url, timeout=10)
        self.log_func = log_func

        self._id = 0
        self._lock = threading.Lock()
        self._pending = {}
        self._running = True

        self.network_events = queue.Queue()
        self.context_ids = []
        self.frame_context_map = {}
        self.default_context_id = None

        self.recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
        self.recv_thread.start()

        self.call("Page.enable", {}, timeout=10)
        self.call("Runtime.enable", {}, timeout=10)
        self.call("Network.enable", {}, timeout=10)

        # 给 Runtime.executionContextCreated 事件一点时间返回，Win7/旧内核上不要太激进。
        time.sleep(0.8)

    def close(self):
        self._running = False
        try:
            self.ws.close()
        except Exception:
            pass

    def _next_id(self):
        with self._lock:
            self._id += 1
            return self._id

    def send(self, method, params=None):
        if params is None:
            params = {}
        mid = self._next_id()
        self.ws.send(json.dumps({"id": mid, "method": method, "params": params}, ensure_ascii=False))
        return mid

    def call(self, method, params=None, timeout=20):
        mid = self.send(method, params or {})
        deadline = time.time() + timeout
        while time.time() < deadline:
            if mid in self._pending:
                msg = self._pending.pop(mid)
                if "error" in msg:
                    raise RuntimeError(f"CDP调用失败 {method}: {msg.get('error')}")
                return msg
            time.sleep(0.02)
        raise TimeoutError(f"CDP调用超时: {method}")

    def _recv_loop(self):
        while self._running:
            try:
                raw = self.ws.recv()
                if not raw:
                    continue
                msg = json.loads(raw)
                if "id" in msg:
                    self._pending[msg["id"]] = msg
                else:
                    self._handle_event(msg)
            except Exception:
                time.sleep(0.05)

    def _handle_event(self, msg):
        method = msg.get("method")
        params = msg.get("params", {})

        if method == "Runtime.executionContextCreated":
            ctx = params.get("context", {})
            cid = ctx.get("id")
            aux = ctx.get("auxData") or {}
            frame_id = aux.get("frameId")
            is_default = aux.get("isDefault")
            if cid and cid not in self.context_ids:
                self.context_ids.append(cid)
            if cid and frame_id and is_default:
                self.frame_context_map[frame_id] = cid
                if self.default_context_id is None:
                    self.default_context_id = cid

        elif method == "Runtime.executionContextsCleared":
            self.context_ids = []
            self.frame_context_map = {}
            self.default_context_id = None

        elif method == "Network.responseReceived":
            response = params.get("response", {})
            url = response.get("url", "")
            if TARGET_API_KEYWORD in url:
                self.network_events.put(params)

    def refresh_contexts(self):
        """重新打开 Runtime，促使 executionContextCreated 事件刷新。"""
        try:
            self.call("Runtime.disable", {}, timeout=5)
            time.sleep(0.2)
            self.context_ids = []
            self.frame_context_map = {}
            self.default_context_id = None
            self.call("Runtime.enable", {}, timeout=5)
            time.sleep(0.8)
        except Exception:
            pass

    def eval(self, expression, context_id=None, timeout=20):
        params = {
            "expression": expression,
            "returnByValue": True,
            "awaitPromise": True,
        }
        if context_id is not None:
            params["contextId"] = int(context_id)
        msg = self.call("Runtime.evaluate", params, timeout=timeout)
        result = msg.get("result", {}).get("result", {})
        if "exceptionDetails" in msg.get("result", {}):
            raise RuntimeError(msg["result"]["exceptionDetails"])
        if result.get("type") == "undefined":
            return None
        return result.get("value")

    def get_response_body(self, request_id, timeout=20):
        msg = self.call("Network.getResponseBody", {"requestId": request_id}, timeout=timeout)
        return msg.get("result", {}).get("body", "")

    def clear_network_events(self):
        try:
            while True:
                self.network_events.get_nowait()
        except queue.Empty:
            pass


# ================= 数据存储 =================
class PageStore:
    """
    保存每一页原始记录，避免由于翻页重抓导致页内重复叠加。
    页面层面覆盖写入，导出时再统一做 38 字段重复标记。
    """

    def __init__(self):
        self.page_rows = {}

    def clear(self):
        self.page_rows.clear()

    def put(self, page_num, rows):
        self.page_rows[int(page_num)] = list(rows)

    def total_rows(self):
        return sum(len(v) for v in self.page_rows.values())

    def flatten(self):
        out = []
        for page_num in sorted(self.page_rows.keys()):
            out.extend(self.page_rows[page_num])
        return out


# ================= 主采集器 =================
class Collector:
    def __init__(self, gui):
        self.gui = gui
        self.driver = None  # 这里保留变量名，实际为 CDPClient，避免改动 GUI/流程语义。
        self.active_context_id = None
        self.page_store = PageStore()
        self.total_count = 0
        self.stop_event = threading.Event()
        self.collecting = False
        self.page_size = PAGE_SIZE

    # --------- 日志 ---------
    def log(self, msg, level="info"):
        self.gui.log_queue.put(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}\n")
        if level == "debug":
            logging.debug(msg)

    # --------- 浏览器/日志 ---------
    def clear_performance_logs(self):
        """
        纯 CDP 版没有 Selenium performance log。
        这里保留原按钮和原流程语义：清空已缓存的 Network 事件队列。
        """
        try:
            if self.driver:
                self.driver.clear_network_events()
        except Exception:
            pass

    def reset_session(self):
        self.page_store.clear()
        self.total_count = 0
        self.stop_event.clear()

    # --------- SGSBrowser/CDP ---------
    def launch_sg_browser(self):
        if not os.path.exists(SG_BROWSER_PATH):
            raise FileNotFoundError(f"未找到国网浏览器: {SG_BROWSER_PATH}")

        _ensure_dir(SG_USER_DATA_DIR)

        if _port_ready(DEBUG_VERSION_URL, timeout=1.2):
            return

        args = [
            SG_BROWSER_PATH,
            f"--remote-debugging-port={DEBUG_PORT}",
            f"--user-data-dir={SG_USER_DATA_DIR}",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-popup-blocking",
        ]

        subprocess.Popen(
            args,
            cwd=os.path.dirname(SG_BROWSER_PATH),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        deadline = time.time() + 20
        while time.time() < deadline:
            if _port_ready(DEBUG_VERSION_URL, timeout=1.0):
                return
            time.sleep(0.4)

        raise RuntimeError("国网浏览器 9222 端口未能在规定时间内启动")

    def _choose_debug_tab(self):
        tabs = _http_get_json(DEBUG_TABS_URL, timeout=5)
        if not isinstance(tabs, list) or not tabs:
            raise RuntimeError("9222端口已打开，但未找到可连接的页面标签页")

        page_tabs = [t for t in tabs if t.get("type") == "page" and t.get("webSocketDebuggerUrl")]
        if not page_tabs:
            raise RuntimeError("未找到带 webSocketDebuggerUrl 的 page 标签页")

        # 优先选当前用户正在看的业务页；如果还没登录/还在空白页，也取第一个 page。
        non_blank = []
        for t in page_tabs:
            url = t.get("url", "") or ""
            if not (url.startswith("chrome://") or url.startswith("devtools://")):
                non_blank.append(t)
        return non_blank[0] if non_blank else page_tabs[0]

    def attach_driver(self):
        """
        原 Selenium/chromedriver 连接点。
        现在改为：读取 /json，取 webSocketDebuggerUrl，直接建立 CDP WebSocket。
        """
        tab = self._choose_debug_tab()
        ws_url = tab.get("webSocketDebuggerUrl")
        if not ws_url:
            raise RuntimeError("当前标签页没有 webSocketDebuggerUrl，无法纯CDP连接")
        self.driver = CDPClient(ws_url, log_func=self.log)
        self.log("✅ 已通过纯CDP连接国网浏览器，不再使用 selenium/chromedriver。")

    def connect_sg_browser(self):
        self.launch_sg_browser()
        self.attach_driver()

    # --------- iframe / 分页盒子 ---------
    def _pagination_box_js(self):
        return r"""
(function(){
const isVisible = (el) => {
  if (!el) return false;
  const s = getComputedStyle(el);
  const r = el.getBoundingClientRect();
  return s.display !== 'none' && s.visibility !== 'hidden' && r.width > 0 && r.height > 0;
};

const candidates = [];
for (const box of Array.from(document.querySelectorAll('.pagination-box, .el-pagination'))) {
  if (!isVisible(box)) continue;
  if (!box.querySelector('.btn-next')) continue;
  if (!box.querySelector('.el-pagination__jump input')) continue;
  if (!box.querySelector('.el-pager')) continue;

  const r = box.getBoundingClientRect();
  candidates.push({
    top: r.top,
    bottom: r.bottom,
    left: r.left,
    right: r.right
  });
}

if (!candidates.length) return false;

candidates.sort((a, b) =>
  (a.bottom - b.bottom) ||
  (a.top - b.top) ||
  (a.left - b.left) ||
  (a.right - b.right)
);

return true;
})();
"""

    def _eval_page_js(self, js, context_id=None, timeout=20):
        if not self.driver:
            raise RuntimeError("CDP未连接")
        return self.driver.eval(js, context_id=context_id, timeout=timeout)

    def _enter_bound_context(self):
        """
        Selenium 版这里 switch_to.frame。
        纯 CDP 版通过 active_context_id 指定 Runtime.evaluate 的执行上下文。
        """
        return self.active_context_id

    def ensure_page_context(self, timeout=10):
        """
        优先使用已绑定 iframe 执行上下文；如果没有绑定，则扫描所有 Runtime context，
        找到包含 ElementUI 分页盒子的上下文。
        """
        deadline = time.time() + timeout

        while time.time() < deadline:
            if self.stop_event.is_set():
                return False

            # 1) 先尝试当前绑定上下文
            try:
                if self.active_context_id is not None:
                    ok = self._eval_page_js(self._pagination_box_js(), context_id=self.active_context_id, timeout=5)
                    if ok:
                        return True
            except Exception:
                pass

            # 2) 刷新 contexts，防止用户刚进入页面/弹窗 iframe 刚创建
            try:
                if not self.driver.context_ids:
                    self.driver.refresh_contexts()
            except Exception:
                pass

            # 3) 扫描所有上下文，包括 iframe 独立执行上下文
            try:
                contexts = list(self.driver.context_ids)
                # 默认上下文优先，其余 iframe 后面扫
                if self.driver.default_context_id in contexts:
                    contexts.remove(self.driver.default_context_id)
                    contexts.insert(0, self.driver.default_context_id)

                for cid in contexts:
                    try:
                        ok = self._eval_page_js(self._pagination_box_js(), context_id=cid, timeout=5)
                        if ok:
                            self.active_context_id = cid
                            return True
                    except Exception:
                        continue
            except Exception:
                pass

            time.sleep(0.4)

        return False

    def get_pagination_box(self):
        """
        保留原函数名。纯CDP版不返回 WebElement，只返回是否找到了分页区。
        """
        try:
            if not self.ensure_page_context(timeout=5):
                return None
            return True
        except Exception:
            return None

    def get_current_page_num(self):
        js = r"""
(function(){
const isVisible = (el) => {
  if (!el) return false;
  const s = getComputedStyle(el);
  const r = el.getBoundingClientRect();
  return s.display !== 'none' && s.visibility !== 'hidden' && r.width > 0 && r.height > 0;
};
const boxes = Array.from(document.querySelectorAll('.pagination-box, .el-pagination')).filter(box =>
  isVisible(box) && box.querySelector('.btn-next') && box.querySelector('.el-pagination__jump input') && box.querySelector('.el-pager')
);
if (!boxes.length) return null;
boxes.sort((a,b)=>a.getBoundingClientRect().bottom-b.getBoundingClientRect().bottom);
const box = boxes[boxes.length-1];
const active = box.querySelector('.el-pager li.active');
if (!active) return null;
const txt = (active.innerText || active.textContent || '').trim();
const n = parseInt(txt, 10);
return isNaN(n) ? null : n;
})();
"""
        try:
            if not self.ensure_page_context(timeout=5):
                return None
            val = self._eval_page_js(js, context_id=self.active_context_id, timeout=5)
            return int(val) if val is not None else None
        except Exception:
            return None

    def wait_for_page_num(self, target_page, timeout=PAGE_CHANGE_TIMEOUT):
        deadline = time.time() + timeout
        target_page = int(target_page)

        while time.time() < deadline and not self.stop_event.is_set():
            try:
                cur = self.get_current_page_num()
                if cur == target_page:
                    return True
            except Exception:
                pass
            time.sleep(0.2)
        return False

    def read_total_from_dom(self):
        js = r"""
(function(){
const isVisible = (el) => {
  if (!el) return false;
  const s = getComputedStyle(el);
  const r = el.getBoundingClientRect();
  return s.display !== 'none' && s.visibility !== 'hidden' && r.width > 0 && r.height > 0;
};
const boxes = Array.from(document.querySelectorAll('.pagination-box, .el-pagination')).filter(box =>
  isVisible(box) && box.querySelector('.el-pagination__total')
);
if (!boxes.length) return null;
boxes.sort((a,b)=>a.getBoundingClientRect().bottom-b.getBoundingClientRect().bottom);
const totalEl = boxes[boxes.length-1].querySelector('.el-pagination__total');
if (!totalEl) return null;
const m = (totalEl.innerText || totalEl.textContent || '').match(/\d+/);
return m ? parseInt(m[0], 10) : null;
})();
"""
        try:
            if not self.ensure_page_context(timeout=5):
                return None
            val = self._eval_page_js(js, context_id=self.active_context_id, timeout=5)
            return int(val) if val is not None else None
        except Exception:
            return None

    # --------- Network 响应捕获 ---------
    def wait_for_latest_query_payload(self, timeout=NETWORK_RESPONSE_TIMEOUT):
        """
        纯 CDP 版：监听 Network.responseReceived，发现 queryTransPqList 后，
        通过 Network.getResponseBody 读取响应体。
        保留原来的思想：页码先变化、表格/Preview.data 稍后才刷新，只以响应体可读为准。
        """
        deadline = time.time() + timeout
        latest_rid = None

        while time.time() < deadline and not self.stop_event.is_set():
            try:
                # 每轮尽量取最新目标响应，避免旧响应误用。
                try:
                    while True:
                        params = self.driver.network_events.get_nowait()
                        latest_rid = params.get("requestId")
                        self.log(f"📡 捕获目标响应: rid={latest_rid}", "debug")
                except queue.Empty:
                    pass

                if latest_rid:
                    try:
                        body = self.driver.get_response_body(latest_rid, timeout=10)
                        if not body:
                            time.sleep(0.35)
                            continue
                        payload = json.loads(body)
                        return payload
                    except Exception as e:
                        err = str(e)
                        if "No resource with given identifier" in err or "resource" in err or "超时" in err or "Timeout" in err:
                            time.sleep(0.35)
                            continue
                        self.log(f"读取接口响应失败: {e}", "debug")

            except Exception:
                pass

            time.sleep(0.25)

        return None

    # --------- payload 解析 ---------
    def extract_rows_from_payload(self, payload):
        if not isinstance(payload, dict):
            return []

        data = payload.get("data")
        if isinstance(data, list):
            return data

        if isinstance(data, dict):
            for key in ("list", "rows", "records", "data"):
                val = data.get(key)
                if isinstance(val, list):
                    return val

        for key in ("list", "rows", "records"):
            val = payload.get(key)
            if isinstance(val, list):
                return val

        return []

    def extract_total_from_payload(self, payload):
        if not isinstance(payload, dict):
            return None

        total = payload.get("total")
        if isinstance(total, int):
            return total
        if isinstance(total, str) and total.isdigit():
            return int(total)

        data = payload.get("data")
        if isinstance(data, dict):
            t = data.get("total")
            if isinstance(t, int):
                return t
            if isinstance(t, str) and t.isdigit():
                return int(t)

            page = data.get("page")
            if isinstance(page, dict):
                t = page.get("total")
                if isinstance(t, int):
                    return t
                if isinstance(t, str) and t.isdigit():
                    return int(t)

        page = payload.get("page")
        if isinstance(page, dict):
            t = page.get("total")
            if isinstance(t, int):
                return t
            if isinstance(t, str) and t.isdigit():
                return int(t)

        return None

    def payload_to_rows(self, payload):
        rows = self.extract_rows_from_payload(payload)
        records = []

        for item in rows:
            rec = {}
            for raw_key, cn in FIELD_MAP:
                rec[cn] = clean_cell(item.get(raw_key, ""))
            records.append(rec)

        return records

    # --------- 翻页盒子操作 ---------
    def jump_to_page(self, target_page):
        js = r"""
(function(targetPage){
const isVisible = (el) => {
  if (!el) return false;
  const s = getComputedStyle(el);
  const r = el.getBoundingClientRect();
  return s.display !== 'none' && s.visibility !== 'hidden' && r.width > 0 && r.height > 0;
};
const boxes = Array.from(document.querySelectorAll('.pagination-box, .el-pagination')).filter(box =>
  isVisible(box) && box.querySelector('.btn-next') && box.querySelector('.el-pagination__jump input') && box.querySelector('.el-pager')
);
if (!boxes.length) return false;
boxes.sort((a,b)=>a.getBoundingClientRect().bottom-b.getBoundingClientRect().bottom);
const box = boxes[boxes.length-1];
const inp = box.querySelector('.el-pagination__jump input');
if (!inp) return false;
inp.scrollIntoView({block:'center', inline:'center'});
inp.focus();
inp.value = String(targetPage);
inp.dispatchEvent(new Event('input', { bubbles: true }));
inp.dispatchEvent(new Event('change', { bubbles: true }));
inp.dispatchEvent(new KeyboardEvent('keydown', { bubbles: true, cancelable: true, key:'Enter', code:'Enter', keyCode:13, which:13 }));
inp.dispatchEvent(new KeyboardEvent('keyup', { bubbles: true, cancelable: true, key:'Enter', code:'Enter', keyCode:13, which:13 }));
return true;
})(%d);
""" % int(target_page)
        try:
            if not self.ensure_page_context(timeout=5):
                return False
            return bool(self._eval_page_js(js, context_id=self.active_context_id, timeout=5))
        except Exception as e:
            self.log(f"跳页失败: {e}", "debug")
            return False

    def click_next_page(self):
        js = r"""
(function(){
const isVisible = (el) => {
  if (!el) return false;
  const s = getComputedStyle(el);
  const r = el.getBoundingClientRect();
  return s.display !== 'none' && s.visibility !== 'hidden' && r.width > 0 && r.height > 0;
};
const boxes = Array.from(document.querySelectorAll('.pagination-box, .el-pagination')).filter(box =>
  isVisible(box) && box.querySelector('.btn-next') && box.querySelector('.el-pagination__jump input') && box.querySelector('.el-pager')
);
if (!boxes.length) return false;
boxes.sort((a,b)=>a.getBoundingClientRect().bottom-b.getBoundingClientRect().bottom);
const box = boxes[boxes.length-1];
const btn = box.querySelector('.btn-next');
if (!btn) return false;
const disabled = btn.getAttribute('disabled');
const klass = btn.getAttribute('class') || '';
if (disabled || klass.indexOf('is-disabled') >= 0) return false;
btn.scrollIntoView({block:'center', inline:'center'});
btn.click();
return true;
})();
"""
        try:
            if not self.ensure_page_context(timeout=5):
                return False
            return bool(self._eval_page_js(js, context_id=self.active_context_id, timeout=5))
        except Exception as e:
            self.log(f"点击下一页失败: {e}", "debug")
            return False

    def goto_page(self, target_page):
        """
        先尝试输入页码跳转，失败则点下一页。
        页码先变，实际展示数据/Preview.data 稍后刷新，因此这里需要：
        1) 先等页码高亮变化
        2) 再给一点 settle delay
        3) 后续由 Network 响应可读来确认数据是否真正稳定
        """
        target_page = int(target_page)
        current = self.get_current_page_num()

        if current == target_page:
            return True

        # 1) 优先直接跳页
        if self.jump_to_page(target_page):
            if self.wait_for_page_num(target_page, timeout=PAGE_CHANGE_TIMEOUT):
                time.sleep(PAGE_SETTLE_DELAY)
                return True

        # 2) 失败时，若已知当前页，则补点下一页到目标页
        current = self.get_current_page_num() or current or 1
        if current < target_page:
            for _ in range(target_page - current):
                if self.stop_event.is_set():
                    return False
                if not self.click_next_page():
                    return False
                current += 1
                if not self.wait_for_page_num(current, timeout=PAGE_CHANGE_TIMEOUT):
                    return False
                time.sleep(PAGE_SETTLE_DELAY)
            return current == target_page

        return False

    # --------- 重复标记 ---------
    def record_key(self, record):
        return tuple(normalize_text(record.get(cn, "")) for _, cn in FIELD_MAP)

    def build_rows_with_repeat_marks(self, rows):
        """
        38字段作为唯一键：
        - 真正去重用于判定重复组，不删除重复行
        - 重复组按首次出现顺序编号 1,2,3...
        - 唯一行最后一列保持空白
        """
        if not rows:
            return []

        counts = Counter(self.record_key(r) for r in rows)
        key_to_group = {}
        next_group = 1
        out = []

        for record in rows:
            key = self.record_key(record)
            repeat_flag = ""
            if counts[key] > 1:
                if key not in key_to_group:
                    key_to_group[key] = str(next_group)
                    next_group += 1
                repeat_flag = key_to_group[key]

            row_values = [record.get(cn, "") for _, cn in FIELD_MAP]
            row_values.append(repeat_flag)
            out.append(row_values)

        return out

    # --------- 存储与导出 ---------
    def store_page(self, page_num, records):
        self.page_store.put(page_num, records)

    def export_excel(self):
        rows = self.page_store.flatten()
        if not rows:
            self.log("没有可导出的数据。")
            return

        out_rows = self.build_rows_with_repeat_marks(rows)
        df = pd.DataFrame(out_rows, columns=EXPORT_COLUMNS)

        out_path = os.path.join(BASE_DIR, f"采集结果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xls")
        df.to_excel(
            out_path,
            index=False,
            engine="xlwt"
        )
        self.log(f"导出成功：{out_path}")
        self.gui.log_queue.put(("DONE", out_path))

    # --------- 主流程 ---------
    def collect(self):
        """
        交互逻辑：
        1. 用户先点击“清空Performance Log”
        2. 用户手工点击查询，第一页完整展示
        3. 点击“开始采集”后，程序读取最新 queryTransPqList 响应
        4. 然后逐页翻页，等待页码变化 + Preview.data 可读
        """
        self.collecting = True
        self.stop_event.clear()
        self.reset_session()

        try:
            if not self.ensure_page_context(timeout=15):
                self.log("未找到分页区域，请先打开目标列表页面并展示第一页。")
                self.gui.log_queue.put("RESET_BTN")
                return

            current_page = self.get_current_page_num()
            if current_page != 1:
                self.log("请确保当前停留在第一页后，再点击开始采集。")
                self.gui.log_queue.put("RESET_BTN")
                return

            self.log(f"等待第一页 {TARGET_API_KEYWORD} 响应体可读 ...")
            payload = self.wait_for_latest_query_payload(timeout=NETWORK_RESPONSE_TIMEOUT)
            if not payload:
                self.log(f"未捕获到第一页 {TARGET_API_KEYWORD} 响应，请确认已先清空日志，再点击查询并完整展示第一页。")
                self.gui.log_queue.put("RESET_BTN")
                return

            records = self.payload_to_rows(payload)
            self.store_page(1, records)

            total = self.extract_total_from_payload(payload)
            if total is None:
                total = self.read_total_from_dom()

            if total is None:
                self.log("无法识别总条数，停止。")
                self.gui.log_queue.put("RESET_BTN")
                return

            self.total_count = int(total)
            max_pages = max(1, math.ceil(self.total_count / self.page_size))

            self.log(f"识别到总条数: {self.total_count}，共 {max_pages} 页")
            self.gui.log_queue.put(("STATUS", 1, len(records), self.total_count))

            # 逐页采集
            for page_num in range(2, max_pages + 1):
                if self.stop_event.is_set():
                    self.log("已停止采集。")
                    break

                self.log(f"跳转到第 {page_num} 页 ...")
                self.clear_performance_logs()

                if not self.goto_page(page_num):
                    self.log(f"第 {page_num} 页翻页失败，尝试重试一次。")
                    self.clear_performance_logs()
                    if not self.goto_page(page_num):
                        self.log(f"第 {page_num} 页仍然失败，跳过该页。")
                        continue

                if not self.wait_for_page_num(page_num, timeout=PAGE_CHANGE_TIMEOUT):
                    self.log(f"第 {page_num} 页页码未确认变化，继续等待网络响应。")

                # 页码已变，但 preview.data 往往稍后才刷新，给一个小 settle
                time.sleep(PAGE_SETTLE_DELAY)

                self.log(f"等待第 {page_num} 页 {TARGET_API_KEYWORD} 响应体可读 ...")
                payload = self.wait_for_latest_query_payload(timeout=NETWORK_RESPONSE_TIMEOUT)

                if not payload:
                    self.log(f"第 {page_num} 页响应超时，重试一次。")
                    self.clear_performance_logs()
                    if not self.goto_page(page_num):
                        self.log(f"第 {page_num} 页二次翻页失败，跳过。")
                        continue
                    payload = self.wait_for_latest_query_payload(timeout=NETWORK_RESPONSE_TIMEOUT)

                if not payload:
                    self.log(f"第 {page_num} 页仍未抓到响应，跳过。")
                    continue

                records = self.payload_to_rows(payload)
                self.store_page(page_num, records)
                self.log(f"[P{page_num}] 抓到 {len(records)} 条，累计 {self.page_store.total_rows()} 条")
                self.gui.log_queue.put(("STATUS", page_num, self.page_store.total_rows(), self.total_count))

            self.export_excel()

        except Exception as e:
            self.log(f"主流程异常：{e}")
            self.gui.log_queue.put("RESET_BTN")
        finally:
            self.collecting = False

    def stop(self):
        self.stop_event.set()
        self.log("收到停止指令。")


# ================= GUI =================
class GUI:
    def __init__(self, root):
        self.root = root
        self.root.title(APP_TITLE)
        self.root.geometry("980x720")
        self.root.configure(bg="#1c1c1c")

        self.log_queue = queue.Queue()
        self.collector = Collector(self)

        self.worker = None

        top = tk.Frame(root, bg="#1c1c1c")
        top.pack(pady=12)

        self.btn_open = tk.Button(top, text="1. 启动国网浏览器", command=self.open_browser, width=18, bg="#333", fg="#0cf")
        self.btn_open.grid(row=0, column=0, padx=8)

        self.btn_clear = tk.Button(top, text="2. 清空Performance Log", command=self.clear_perf_log, width=20, bg="#333", fg="#ff0")
        self.btn_clear.grid(row=0, column=1, padx=8)

        self.btn_start = tk.Button(top, text="3. 开始采集", command=self.start_collect, width=16, bg="#333", fg="#0f6")
        self.btn_start.grid(row=0, column=2, padx=8)

        self.btn_stop = tk.Button(top, text="停止采集", command=self.stop_collect, width=12, bg="#333", fg="#f66")
        self.btn_stop.grid(row=0, column=3, padx=8)

        self.status = tk.Label(
            root,
            text="等待操作：先启动国网浏览器，再手工登录并打开目标页面。",
            fg="#0f6",
            bg="#1c1c1c",
            font=("Consolas", 11, "bold"),
            wraplength=920,
            justify="left",
        )
        self.status.pack(pady=8)

        self.text = scrolledtext.ScrolledText(
            root, height=32, width=130, bg="#000", fg="#0f6", font=("Consolas", 9)
        )
        self.text.pack(pady=10)

        self.root.after(100, self.process_queue)

    def set_busy(self, busy=True):
        if busy:
            self.btn_open.config(state="disabled")
            self.btn_clear.config(state="disabled")
            self.btn_start.config(state="disabled")
            self.btn_stop.config(state="normal")
        else:
            self.btn_open.config(state="normal")
            self.btn_clear.config(state="normal")
            self.btn_start.config(state="normal")
            self.btn_stop.config(state="disabled")

    def open_browser(self):
        try:
            self.status.config(text="正在启动/连接国网浏览器 9222 端口 ...")
            self.collector.connect_sg_browser()
            self.status.config(text="国网浏览器已连接。请手工登录并打开目标列表页面。")
            self.text.insert(tk.END, "✅ 国网浏览器已启动/连接，请手工登录并打开目标列表页面。\n")
            self.text.see(tk.END)

        except Exception as e:
            messagebox.showerror("错误", f"浏览器开启失败: {e}")

    def clear_perf_log(self):
        if not self.collector.driver:
            messagebox.showwarning("提示", "请先启动国网浏览器。")
            return

        try:
            self.collector.clear_performance_logs()
            self.collector.stop_event.clear()
            self.status.config(text="performance log 已清除，请点击查询按钮并完整展示第一页的数据，然后点击开始采集。")
            self.text.insert(tk.END, "✅ performance log 已清除，请点击查询按钮并完整展示第一页的数据。\n")
            self.text.see(tk.END)
        except Exception as e:
            messagebox.showerror("错误", f"清空日志失败: {e}")

    def start_collect(self):
        if not self.collector.driver:
            messagebox.showwarning("提示", "请先启动国网浏览器。")
            return

        if self.collector.collecting:
            messagebox.showwarning("提示", "正在采集中。")
            return

        self.set_busy(True)
        self.status.config(text=f"开始采集：等待第一页 {TARGET_API_KEYWORD} 响应并自动翻页。")
        self.worker = threading.Thread(target=self.collector.collect, daemon=True)
        self.worker.start()

    def stop_collect(self):
        self.collector.stop()
        self.status.config(text="已请求停止。")
        self.text.insert(tk.END, "⏹ 已请求停止采集。\n")
        self.text.see(tk.END)

    def process_queue(self):
        try:
            while True:
                item = self.log_queue.get_nowait()

                if isinstance(item, tuple):
                    tag = item[0]
                    if tag == "STATUS":
                        _, page_num, cur_rows, total = item
                        self.status.config(text=f"页码 {page_num} | 已采集 {cur_rows} / {total}")
                    elif tag == "DONE":
                        _, path = item
                        self.set_busy(False)
                        self.status.config(text=f"采集完成：{path}")
                        messagebox.showinfo("完成", f"数据已导出：\n{path}")
                    continue

                if item == "RESET_BTN":
                    self.set_busy(False)
                    continue

                self.text.insert(tk.END, item)
                self.text.see(tk.END)

        except queue.Empty:
            pass

        if self.worker and not self.worker.is_alive():
            self.set_busy(False)
            self.collector.collecting = False

        self.root.after(100, self.process_queue)


if __name__ == "__main__":
    root = tk.Tk()
    app = GUI(root)
    root.mainloop()

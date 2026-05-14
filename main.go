#!/usr/bin/env python3
# -- coding: utf-8 --

import sys
import os
import time
import math
import json
import threading
import logging
import queue
from datetime import datetime
from decimal import Decimal, InvalidOperation

import pandas as pd
import tkinter as tk
from tkinter import messagebox, scrolledtext

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# ================= 基础配置 =================
BASE_DIR = os.path.dirname(os.path.abspath(sys.argv[0]))
CHROMEDRIVER_PATH = os.path.join(BASE_DIR, "chromedriver.exe")
CHROME_BINARY_PATH = os.path.join(BASE_DIR, "Chrome-bin", "chrome.exe")

TARGET_API_KEYWORD = "queryTransPqList"
PAGE_SIZE = 100
NETWORK_RESPONSE_TIMEOUT = 45  # 页面展示虽快，但 preview.data 可能延迟，适当放宽
PAGE_CHANGE_TIMEOUT = 12

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

EXPORT_COLUMNS = [cn for _, cn in FIELD_MAP]


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


# ================= 数据存储 =================
class PageStore:
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

    def pages(self):
        return sorted(self.page_rows.keys())


# ================= 主采集器 =================
class Collector:
    def __init__(self, gui):
        self.gui = gui
        self.driver = None
        self.active_frame_index = None  # 绑定到目标 iframe 的索引
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
        try:
            self.driver.get_log("performance")
        except Exception:
            pass

    def reset_session(self):
        self.page_store.clear()
        self.total_count = 0
        self.stop_event.clear()

    # --------- iframe / 分页盒子 ---------
    def _pagination_box_js(self):
        return r"""
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
    box,
    top: r.top,
    bottom: r.bottom,
    left: r.left,
    right: r.right
  });
}

if (!candidates.length) return null;

candidates.sort((a, b) =>
  (a.bottom - b.bottom) ||
  (a.top - b.top) ||
  (a.left - b.left) ||
  (a.right - b.right)
);

return candidates[candidates.length - 1].box;
"""

    def _enter_bound_context(self):
        self.driver.switch_to.default_content()
        if self.active_frame_index is not None:
            self.driver.switch_to.frame(self.active_frame_index)

    def _page_has_pagination(self):
        try:
            self._enter_bound_context()
            return bool(self.driver.execute_script(self._pagination_box_js()))
        except Exception:
            return False

    def ensure_page_context(self, timeout=10):
        """
        优先使用已绑定 iframe；
        如果没有绑定，则扫描所有 iframe，找到包含 pagination-box 的那个。
        """
        deadline = time.time() + timeout

        while time.time() < deadline:
            # 1) 先尝试当前绑定上下文
            try:
                if self.active_frame_index is not None:
                    self._enter_bound_context()
                    if self.driver.execute_script(self._pagination_box_js()):
                        return True
            except Exception:
                pass

            # 2) 尝试默认文档
            try:
                self.driver.switch_to.default_content()
                if self.driver.execute_script(self._pagination_box_js()):
                    self.active_frame_index = None
                    return True
            except Exception:
                pass

            # 3) 扫描 iframes
            try:
                self.driver.switch_to.default_content()
                iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
                for i in range(len(iframes)):
                    try:
                        self.driver.switch_to.default_content()
                        self.driver.switch_to.frame(i)
                        if self.driver.execute_script(self._pagination_box_js()):
                            self.active_frame_index = i
                            return True
                    except Exception:
                        continue
            except Exception:
                pass

            time.sleep(0.4)

        return False

    def get_pagination_box(self):
        try:
            if not self.ensure_page_context(timeout=5):
                return None
            self._enter_bound_context()
            return self.driver.execute_script(self._pagination_box_js())
        except Exception:
            return None

    def get_current_page_num(self):
        try:
            box = self.get_pagination_box()
            if not box:
                return None
            active = box.find_elements(By.CSS_SELECTOR, ".el-pager li.active")
            if not active:
                return None
            txt = active[0].text.strip()
            return int(txt) if txt.isdigit() else None
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
        try:
            box = self.get_pagination_box()
            if not box:
                return None
            total_el = box.find_element(By.CSS_SELECTOR, ".el-pagination__total")
            m = __import__("re").search(r"\d+", total_el.text)
            return int(m.group()) if m else None
        except Exception:
            return None

    # --------- Network 响应捕获 ---------
    def wait_for_latest_query_payload(self, timeout=NETWORK_RESPONSE_TIMEOUT):
        """
        轮询 performance log，等待最新的 queryTransPqList 响应体可读。
        """
        deadline = time.time() + timeout
        latest_rid = None

        while time.time() < deadline and not self.stop_event.is_set():
            # 收集最新 requestId
            try:
                logs = self.driver.get_log("performance")
            except Exception:
                logs = []

            for entry in logs:
                try:
                    msg = json.loads(entry["message"])["message"]
                except Exception:
                    continue

                if msg.get("method") != "Network.responseReceived":
                    continue

                params = msg.get("params", {})
                url = params.get("response", {}).get("url", "")
                if TARGET_API_KEYWORD in url:
                    latest_rid = params.get("requestId")
                    self.log(f"📡 捕获目标响应: rid={latest_rid}", "debug")

            if latest_rid:
                try:
                    res = self.driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": latest_rid})
                    body = res.get("body", "")
                    payload = json.loads(body)
                    return payload
                except Exception as e:
                    err = str(e)
                    if "No resource with given identifier found" in err or "timed out" in err:
                        time.sleep(0.35)
                        continue
                    self.log(f"读取接口响应失败: {e}", "debug")

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
        try:
            if not self.ensure_page_context(timeout=5):
                return False
            self._enter_bound_context()

            box = self.get_pagination_box()
            if not box:
                return False

            inp = box.find_element(By.CSS_SELECTOR, ".el-pagination__jump input")
            self.driver.execute_script("arguments[0].scrollIntoView({block:'center', inline:'center'});", inp)
            self.driver.execute_script("arguments[0].focus();", inp)
            self.driver.execute_script("arguments[0].value = arguments[1];", inp, str(target_page))
            self.driver.execute_script("arguments[0].dispatchEvent(new Event('input', { bubbles: true }));", inp)
            self.driver.execute_script("arguments[0].dispatchEvent(new Event('change', { bubbles: true }));", inp)
            self.driver.execute_script(
                "arguments[0].dispatchEvent(new KeyboardEvent('keydown', { bubbles: true, cancelable: true, key:'Enter', code:'Enter', keyCode:13, which:13 }));",
                inp,
            )
            self.driver.execute_script(
                "arguments[0].dispatchEvent(new KeyboardEvent('keyup', { bubbles: true, cancelable: true, key:'Enter', code:'Enter', keyCode:13, which:13 }));",
                inp,
            )
            return True
        except Exception as e:
            self.log(f"跳页失败: {e}", "debug")
            return False

    def click_next_page(self):
        try:
            if not self.ensure_page_context(timeout=5):
                return False
            self._enter_bound_context()

            box = self.get_pagination_box()
            if not box:
                return False

            btn = box.find_element(By.CSS_SELECTOR, ".btn-next")
            disabled = btn.get_attribute("disabled")
            klass = btn.get_attribute("class") or ""
            if disabled or "is-disabled" in klass:
                return False

            self.driver.execute_script("arguments[0].scrollIntoView({block:'center', inline:'center'});", btn)
            self.driver.execute_script("arguments[0].click();", btn)
            return True
        except Exception as e:
            self.log(f"点击下一页失败: {e}", "debug")
            return False

    def goto_page(self, target_page):
        """
        先尝试输入页码跳转，失败则点下一页。
        等待页码高亮真正变化后再继续。
        """
        target_page = int(target_page)
        current = self.get_current_page_num()

        if current == target_page:
            return True

        # 1) 优先直接跳页
        if self.jump_to_page(target_page):
            if self.wait_for_page_num(target_page, timeout=PAGE_CHANGE_TIMEOUT):
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
            return current == target_page

        return False

    # --------- 存储与导出 ---------
    def store_page(self, page_num, records):
        self.page_store.put(page_num, records)

    def export_excel(self):
        rows = self.page_store.flatten()
        if not rows:
            self.log("没有可导出的数据。")
            return

        df = pd.DataFrame(rows, columns=EXPORT_COLUMNS)
        out_path = os.path.join(BASE_DIR, f"采集结果_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
        df.to_excel(out_path, index=False)
        self.log(f"导出成功：{out_path}")
        self.gui.log_queue.put(("DONE", out_path))

    # --------- 主流程 ---------
    def collect(self):
        """
        交互逻辑：
        1. 用户先点击“清空日志”
        2. 用户手工点击查询，第一页完整展示
        3. 点击“开始采集”后，程序读取最新 queryTransPqList 响应
        4. 然后逐页翻页，等待页码变化 + Network 响应体可读
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

            self.log("等待第一页 queryTransPqList 响应体可读 ...")
            payload = self.wait_for_latest_query_payload(timeout=NETWORK_RESPONSE_TIMEOUT)
            if not payload:
                self.log("未捕获到第一页 queryTransPqList 响应，请确认已先清空日志，再点击查询并完整展示第一页。")
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

                self.log(f"等待第 {page_num} 页 queryTransPqList 响应体可读 ...")
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
        self.root.title("Web列表采集工具 - Network版")
        self.root.geometry("920x680")
        self.root.configure(bg="#1c1c1c")

        self.log_queue = queue.Queue()
        self.collector = Collector(self)

        self.worker = None

        top = tk.Frame(root, bg="#1c1c1c")
        top.pack(pady=12)

        self.btn_open = tk.Button(top, text="1. 启动浏览器", command=self.open_browser, width=16, bg="#333", fg="#0cf")
        self.btn_open.grid(row=0, column=0, padx=8)

        self.btn_clear = tk.Button(top, text="2. 清空Performance Log", command=self.clear_perf_log, width=20, bg="#333", fg="#ff0")
        self.btn_clear.grid(row=0, column=1, padx=8)

        self.btn_start = tk.Button(top, text="3. 开始采集", command=self.start_collect, width=16, bg="#333", fg="#0f6")
        self.btn_start.grid(row=0, column=2, padx=8)

        self.btn_stop = tk.Button(top, text="停止采集", command=self.stop_collect, width=12, bg="#333", fg="#f66")
        self.btn_stop.grid(row=0, column=3, padx=8)

        self.status = tk.Label(
            root,
            text="等待操作：先启动浏览器，再手工登录并打开目标页面。",
            fg="#0f6",
            bg="#1c1c1c",
            font=("Consolas", 11, "bold"),
            wraplength=860,
            justify="left",
        )
        self.status.pack(pady=8)

        self.text = scrolledtext.ScrolledText(
            root, height=30, width=120, bg="#000", fg="#0f6", font=("Consolas", 9)
        )
        self.text.pack(pady=10)

        self.root.after(100, self.process_queue)

    def set_busy(self, busy=True):
        state = "disabled" if busy else "normal"
        self.btn_open.config(state=state if not busy else "disabled")
        self.btn_clear.config(state=state if not busy else "disabled")
        self.btn_start.config(state=state if not busy else "disabled")
        self.btn_stop.config(state="normal" if busy else "disabled")

    def open_browser(self):
        try:
            if not os.path.exists(CHROME_BINARY_PATH):
                raise FileNotFoundError(f"未找到便携版 Chrome: {CHROME_BINARY_PATH}")
            if not os.path.exists(CHROMEDRIVER_PATH):
                raise FileNotFoundError(f"未找到 chromedriver.exe: {CHROMEDRIVER_PATH}")

            options = Options()
            options.binary_location = CHROME_BINARY_PATH
            options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

            service = Service(executable_path=CHROMEDRIVER_PATH)
            self.collector.driver = webdriver.Chrome(service=service, options=options)
            self.collector.driver.execute_cdp_cmd("Network.enable", {})

            self.status.config(text="浏览器已启动。请手工登录并打开目标列表页面。")
            self.text.insert(tk.END, "✅ 浏览器已启动，请手工登录并打开目标列表页面。\n")
            self.text.see(tk.END)

        except Exception as e:
            messagebox.showerror("错误", f"浏览器开启失败: {e}")

    def clear_perf_log(self):
        if not self.collector.driver:
            messagebox.showwarning("提示", "请先启动浏览器。")
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
            messagebox.showwarning("提示", "请先启动浏览器。")
            return

        if self.collector.collecting:
            messagebox.showwarning("提示", "正在采集中。")
            return

        self.set_busy(True)
        self.status.config(text="开始采集：等待第一页 queryTransPqList 响应并自动翻页。")
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

                self.text.insert(tk.END, item)
                self.text.see(tk.END)

        except queue.Empty:
            pass

        # 如果 worker 已经结束，恢复按钮
        if self.worker and not self.worker.is_alive():
            self.set_busy(False)
            self.collector.collecting = False

        self.root.after(100, self.process_queue)


if __name__ == "__main__":
    root = tk.Tk()
    app = GUI(root)
    root.mainloop()

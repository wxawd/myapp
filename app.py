# -*- coding: utf-8 -*-
"""
图书馆管理系统（Flask + gunicorn + MySQL 主从 + Redis 写缓冲）
与《高可用多节点 Web 服务系统部署手册》配套：

  - GET  /              静态管理页面（static/index.html）
  - GET  /api/books     所有书本；可借数 = MySQL 库存 − Redis 缓冲中的借阅笔数
  - POST /api/books     新增书本（直写 MySQL）
  - POST /api/borrow    借阅：平时直写 MySQL；高峰期写 Redis，后台批量落库
  - GET  /api/borrows   借阅名单（近 100 条）
  - GET  /health       健康检查

写缓冲模型（write-behind）：
  1. 缓冲队列 lib:borrow:queue 长度 >= HIGH_WATERMARK（默认 10）
     → 设置 lib:burst 标志，进入缓冲模式，新借阅只写 Redis（快速返回 202）
  2. 后台线程每 FLUSH_INTERVAL 秒（默认 5s）运行一次 flush：
     - 用 Redis 锁保证多 worker 中只有一个线程落库
     - 把队列整体取出，单事务写入 MySQL（库存-1 + 借阅记录）
     - 队列清空后删除 lib:burst，退出缓冲模式
  3. MySQL 直写失败（如主库短暂不可达）自动降级为缓冲写入，不丢单。

注意：这是演示级实现——队列取出后、落库前进程崩溃会丢失这部分缓冲
（生产环境应加补偿：落库失败时把记录放回队列或写入本地日志文件）。

运行方式：
  venv/bin/pip install -r requirements.txt   # flask/gunicorn/PyMySQL/redis
  venv/bin/gunicorn -w 2 -b 127.0.0.1:8000 app:app
"""
import json
import logging
import os
import socket
import threading
import time
from datetime import datetime

import pymysql
import redis
from flask import Flask, jsonify, request, send_from_directory

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "app.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("library")

MYSQL_CONF = dict(
    host="192.168.140.200",          # 主库 node1（演示读写都走主；从库为热备）
    user="myapp",
    password="Repl@123456",
    database="library",
    charset="utf8mb4",
)
REDIS_CONF = dict(
    host="192.168.140.201",          # Redis node2
    port=6379,
    password="Redis@123456",
)

# ---- 写缓冲参数 ----
HIGH_WATERMARK = 10        # 队列达到该长度即进入缓冲模式
FLUSH_INTERVAL = 5         # 落库线程的扫描周期（秒）
FLUSH_LOCK_TTL = 30        # 落库锁的过期时间（秒），防持锁者崩溃死锁

QUEUE_KEY = "lib:borrow:queue"
BURST_KEY = "lib:burst"
LOCK_KEY = "lib:flush:lock"


def redis_client():
    """每次新建短连接，避免 fork 后共享连接出错（gunicorn sync worker 场景）"""
    return redis.Redis(**REDIS_CONF, decode_responses=True,
                       socket_connect_timeout=2, socket_timeout=3)


def pending_key(book_id):
    return f"lib:pending:{book_id}"


def db():
    return pymysql.connect(**MYSQL_CONF, connect_timeout=3, autocommit=False)


app = Flask(__name__, static_folder=os.path.join(BASE_DIR, "static"),
            static_url_path="/static")


# ---------------------------------------------------------------- 读路径

def query_books():
    conn = db()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute("SELECT id, title, author, total, available "
                        "FROM books ORDER BY id")
            books = cur.fetchall()
    finally:
        conn.close()

    r = redis_client()
    buffered_total = 0
    for b in books:
        pending = int(r.get(pending_key(b["id"])) or 0)
        b["buffered"] = pending
        b["available"] = max(b["available"] - pending, 0)   # 展示口径扣掉缓冲
        buffered_total += pending
    r.close()
    return books, buffered_total


@app.route("/")
def index():
    logger.info("GET / from %s", socket.gethostname())
    return send_from_directory(os.path.join(BASE_DIR, "static"), "index.html")


@app.route("/api/books")
def api_books():
    books, buffered_total = query_books()
    r = redis_client()
    burst = r.get(BURST_KEY) == "1"
    r.close()
    return jsonify({
        "hostname": socket.gethostname(),
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "burst": burst,
        "buffered_total": buffered_total,
        "count": len(books),
        "books": books,
    })


@app.route("/api/borrows")
def api_borrows():
    conn = db()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(
                "SELECT r.id, b.title, r.borrower, r.borrow_time, r.status "
                "FROM borrow_records r JOIN books b ON b.id = r.book_id "
                "ORDER BY r.id DESC LIMIT 100")
            records = cur.fetchall()
    finally:
        conn.close()
    return jsonify({"hostname": socket.gethostname(), "count": len(records),
                    "records": records})


# ---------------------------------------------------------------- 写路径

def insert_borrow_mysql(cur, book_id, borrower, when):
    """事务内执行：库存-1（带余量保护）+ 插入借阅记录"""
    cur.execute("UPDATE books SET available = available - 1 "
                "WHERE id = %s AND available > 0", (book_id,))
    if cur.rowcount == 0:
        raise RuntimeError(f"book {book_id} out of stock")
    cur.execute(
        "INSERT INTO borrow_records(book_id, borrower, borrow_time, status) "
        "VALUES (%s, %s, %s, 'borrowed')",
        (book_id, borrower, when))


def buffer_borrow(r, record):
    pipe = r.pipeline()
    pipe.lpush(QUEUE_KEY, json.dumps(record, ensure_ascii=False))
    pipe.incr(pending_key(record["book_id"]))
    pipe.execute()


@app.route("/api/books", methods=["POST"])
def add_book():
    data = request.get_json(force=True)
    title = (data.get("title") or "").strip()
    author = (data.get("author") or "").strip()
    total = int(data.get("total") or 1)
    if not title or not author or total < 1:
        return jsonify({"ok": False, "msg": "书名、作者必填，册数至少 1"}), 400

    conn = db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO books(title, author, total, available) "
                "VALUES (%s, %s, %s, %s)", (title, author, total, total))
        conn.commit()
    finally:
        conn.close()
    logger.info("ADD book %s / %s x%s", title, author, total)
    return jsonify({"ok": True, "msg": "已添加"})


@app.route("/api/borrow", methods=["POST"])
def borrow():
    data = request.get_json(force=True)
    book_id = int(data.get("book_id"))
    borrower = (data.get("borrower") or "匿名读者").strip()[:50]
    when = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 1) 余量校验：MySQL 库存 − Redis 缓冲笔数
    conn = db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT available FROM books WHERE id = %s", (book_id,))
            row = cur.fetchone()
        available = row[0] if row else 0
    finally:
        conn.close()

    r = redis_client()
    pending = int(r.get(pending_key(book_id)) or 0)
    if available - pending <= 0:
        r.close()
        return jsonify({"ok": False, "msg": "库存不足"}), 409

    record = {"book_id": book_id, "borrower": borrower, "time": when}
    qlen = r.llen(QUEUE_KEY)
    in_burst = r.get(BURST_KEY) == "1"

    # 2) 高峰期判定：已在缓冲模式 或 队列积压到水位 → 写 Redis
    if in_burst or qlen >= HIGH_WATERMARK:
        r.set(BURST_KEY, "1")
        buffer_borrow(r, record)
        r.close()
        logger.info("BORROW buffered book=%s borrower=%s qlen=%s",
                    book_id, borrower, qlen + 1)
        return jsonify({"ok": True, "mode": "buffered",
                        "msg": "已受理（高峰期缓冲中）"}), 202

    # 3) 平时：直写 MySQL；失败则降级为缓冲
    try:
        conn = db()
        try:
            with conn.cursor() as cur:
                insert_borrow_mysql(cur, book_id, borrower, when)
            conn.commit()
        finally:
            conn.close()
        r.close()
        logger.info("BORROW direct book=%s borrower=%s", book_id, borrower)
        return jsonify({"ok": True, "mode": "direct", "msg": "借阅成功"})
    except Exception as exc:
        logger.warning("direct write failed (%s), degrade to buffer", exc)
        r.set(BURST_KEY, "1")
        buffer_borrow(r, record)
        r.close()
        return jsonify({"ok": True, "mode": "buffered",
                        "msg": "已受理（缓冲中）"}), 202


# ---------------------------------------------------------------- 落库线程

def flush_once():
    r = redis_client()
    try:
        # 只有拿到锁的 worker 才落库（SET NX + TTL，防崩溃死锁）
        if not r.set(LOCK_KEY, socket.gethostname(), nx=True, ex=FLUSH_LOCK_TTL):
            return
        records = r.lrange(QUEUE_KEY, 0, -1)
        if not records:
            r.delete(BURST_KEY)          # 队列常空 = 高峰已过，退出缓冲模式
            return

        parsed = [json.loads(x) for x in records]
        # 先出队再落库（演示级取舍，见文件头注释）
        pipe = r.pipeline()
        pipe.delete(QUEUE_KEY)
        for p in parsed:
            pipe.decr(pending_key(p["book_id"]))
        pipe.execute()

        conn = db()
        try:
            with conn.cursor() as cur:
                for p in parsed:
                    insert_borrow_mysql(cur, p["book_id"], p["borrower"],
                                        p["time"])
            conn.commit()
            logger.info("FLUSH %d buffered borrows to MySQL", len(parsed))
        finally:
            conn.close()

        if r.llen(QUEUE_KEY) == 0:
            r.delete(BURST_KEY)
    except Exception:
        logger.exception("flush failed")
    finally:
        r.delete(LOCK_KEY)
        r.close()


def flush_loop():
    while True:
        time.sleep(FLUSH_INTERVAL)
        try:
            flush_once()
        except Exception:
            logger.exception("flush loop error")


# 每个 worker 启动一个落库线程；真正的落库者由 Redis 锁唯一选出
threading.Thread(target=flush_loop, daemon=True).start()


@app.route("/health")
def health():
    return jsonify({"status": "ok", "hostname": socket.gethostname()}), 200


@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "not found"}), 404


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8000, debug=False)

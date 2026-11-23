# -*- coding: utf-8 -*-
"""
商品展示 Demo 后端（Flask + gunicorn）
与《高可用多节点 Web 服务系统部署手册》配套：
  - /            返回静态商品页 index.html
  - /api/products 返回商品 JSON（含主机名，便于观察 LB 轮询效果）
  - /health      健康检查，供 LB 的 chk_nginx / 巡检使用

运行方式：
  pip install -r requirements.txt
  gunicorn -w 2 -b 127.0.0.1:8000 app:app
"""
import logging
import os
import socket
from datetime import datetime

from flask import Flask, jsonify, send_from_directory

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
logger = logging.getLogger("myapp")

app = Flask(__name__, static_folder=BASE_DIR, static_url_path="")

# 演示用商品数据（真实项目应改为从 MySQL 读取）
PRODUCTS = [
    {"id": 1, "name": "机械键盘 K87", "description": "87 键热插拔，三模连接", "price": 349.0, "stock": 42},
    {"id": 2, "name": "无线鼠标 M320", "description": "静音微动，双模 2.4G/蓝牙", "price": 89.0, "stock": 128},
    {"id": 3, "name": "27 寸 4K 显示器", "description": "IPS 面板，Type-C 一线连", "price": 1599.0, "stock": 7},
    {"id": 4, "name": "USB-C 扩展坞", "description": "七合一，千兆网口", "price": 199.0, "stock": 0},
    {"id": 5, "name": "降噪耳机 H5", "description": "主动降噪，40 小时续航", "price": 599.0, "stock": 23},
    {"id": 6, "name": "氮化镓充电器 65W", "description": "双 C 口，折叠插脚", "price": 129.0, "stock": 66},
]


@app.route("/")
def index():
    logger.info("GET / from %s", socket.gethostname())
    return send_from_directory(BASE_DIR, "index.html")


@app.route("/api/products")
def api_products():
    logger.info("GET /api/products from %s", socket.gethostname())
    return jsonify({
        "hostname": socket.gethostname(),
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(PRODUCTS),
        "products": PRODUCTS,
    })


@app.route("/health")
def health():
    """健康检查端点：返回 200 即表示应用存活。"""
    return jsonify({"status": "ok", "hostname": socket.gethostname()}), 200


@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "not found"}), 404


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8000, debug=False)

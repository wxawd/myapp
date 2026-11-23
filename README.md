# 高可用多节点 Web 服务系统 - 商品 Demo

《高可用多节点 Web 服务系统部署手册》配套的示例应用：商品列表展示。

## 文件说明

| 文件 | 说明 |
|---|---|
| `index.html` | 商品列表静态页面（前端） |
| `app.py` | Flask 后端：提供 `/` 静态页、`/api/products` 商品接口、`/health` 健康检查 |
| `requirements.txt` | Python 依赖清单 |

## 本地运行

```bash
pip install -r requirements.txt
gunicorn -w 2 -b 127.0.0.1:8000 app:app
```

浏览器访问 http://127.0.0.1:8000/ ，多刷新几次可看到右上角"后端节点"随不同节点变化（配合 Nginx 负载均衡观察轮询效果）。

## 接口

| 路径 | 说明 |
|---|---|
| `GET /` | 商品展示页 |
| `GET /api/products` | 商品 JSON 数据（含后端主机名与时间戳） |
| `GET /health` | 健康检查，返回 `{"status":"ok"}` |

## 部署

按部署手册第 5 章：推送到 Git 后，在 node1/node2 上 `git pull`，用 systemd（myapp.service）管理 gunicorn。
应用日志写入 `logs/app.log`，由 Filebeat 采集。

# translate-stat-app

一个基于 Node.js 和 `better-sqlite3` 的统计服务：

- `POST /api/events` 接收翻译统计数据并按天写入 SQLite
- `GET /api/dashboard-data` 返回多维聚合结果
- `GET /api/options` 返回筛选项
- `GET /` 提供可视化看板
- 自动清理 30 天前的数据文件

## 启动

```bash
npm start
```

默认启动地址：`http://127.0.0.1:3000/`

## PM2 启动

```bash
cd /path/to/translate-stat-app
npm install
pm2 start ecosystem.config.js
```

常用命令：

```bash
pm2 logs translate-stat-app
pm2 restart translate-stat-app
pm2 stop translate-stat-app
pm2 save
```

可选环境变量：

- `PORT`: 服务端口，默认 `3000`
- `HOST`: 监听地址，默认 `0.0.0.0`
- `APP_TIMEZONE`: 统计按天分库所用时区，默认 `Asia/Shanghai`
- `RETENTION_DAYS`: 数据保留天数，默认 `30`
- `DATA_DIR`: SQLite 文件目录，默认 `<project>/data`

## 写入示例

```bash
curl -X POST http://127.0.0.1:3000/api/events \
  -H "Content-Type: application/json" \
  -d '{
    "app": "desktop-app",
    "provider": "openai",
    "success": true,
    "duration_ms": 128,
    "ts": 1774573200000,
    "app_version": "1.0.0",
    "username": "alice"
  }'
```

批量写入：

```bash
curl -X POST http://127.0.0.1:3000/api/events \
  -H "Content-Type: application/json" \
  -d '{
    "events": [
      {
        "app": "desktop-app",
        "provider": "openai",
        "success": true,
        "duration_ms": 128,
        "ts": 1774573200000,
        "app_version": "1.0.0",
        "username": "alice"
      },
      {
        "app": "mobile-app",
        "provider": "deepl",
        "success": false,
        "duration_ms": 344,
        "ts": 1774576800000,
        "app_version": "2.3.1",
        "username": "bob"
      }
    ]
  }'
```

## 查询示例

```bash
curl "http://127.0.0.1:3000/api/dashboard-data?from=2026-03-01&to=2026-03-27&provider=openai"
```

支持筛选参数：

- `from`, `to`: 日期范围，格式 `YYYY-MM-DD`
- `app`
- `provider`
- `username`
- `app_version`
- `success`: `all` / `true` / `false`

## 测试

```bash
npm test
```

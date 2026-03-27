const fs = require('node:fs');
const http = require('node:http');
const path = require('node:path');

const { DATA_DIR, HOST, PORT, RETENTION_DAYS, TIME_ZONE } = require('./config');
const { StatsStore, ValidationError } = require('./store');

const publicDir = path.join(process.cwd(), 'public');
const staticFiles = new Map([
  ['/', { fileName: 'index.html', contentType: 'text/html; charset=utf-8' }],
  ['/index.html', { fileName: 'index.html', contentType: 'text/html; charset=utf-8' }],
  ['/app.js', { fileName: 'app.js', contentType: 'application/javascript; charset=utf-8' }],
  ['/styles.css', { fileName: 'styles.css', contentType: 'text/css; charset=utf-8' }]
]);

const store = new StatsStore({
  dataDir: DATA_DIR,
  timeZone: TIME_ZONE,
  retentionDays: RETENTION_DAYS
});

store.initialize();

const server = http.createServer(async (req, res) => {
  try {
    setCorsHeaders(res);

    if (req.method === 'OPTIONS') {
      res.writeHead(204);
      res.end();
      return;
    }

    const requestUrl = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    const route = `${req.method} ${requestUrl.pathname}`;

    if (route === 'GET /healthz') {
      sendJson(res, 200, {
        ok: true,
        time_zone: TIME_ZONE,
        retention_days: RETENTION_DAYS
      });
      return;
    }

    if (route === 'POST /api/events') {
      const body = await readJsonBody(req);
      const result = store.insertEvents(body);
      sendJson(res, 201, {
        ok: true,
        ...result
      });
      return;
    }

    if (route === 'GET /api/options') {
      const result = store.getOptions({
        from: requestUrl.searchParams.get('from'),
        to: requestUrl.searchParams.get('to')
      });
      sendJson(res, 200, result);
      return;
    }

    if (route === 'GET /api/dashboard-data') {
      const result = store.getDashboardData({
        from: requestUrl.searchParams.get('from'),
        to: requestUrl.searchParams.get('to'),
        app: requestUrl.searchParams.get('app'),
        provider: requestUrl.searchParams.get('provider'),
        username: requestUrl.searchParams.get('username'),
        app_version: requestUrl.searchParams.get('app_version'),
        success: requestUrl.searchParams.get('success')
      });
      sendJson(res, 200, result);
      return;
    }

    if (req.method === 'GET' && staticFiles.has(requestUrl.pathname)) {
      const asset = staticFiles.get(requestUrl.pathname);
      const filePath = path.join(publicDir, asset.fileName);
      const content = fs.readFileSync(filePath);
      res.writeHead(200, { 'Content-Type': asset.contentType });
      res.end(content);
      return;
    }

    sendJson(res, 404, {
      ok: false,
      error: 'Not Found'
    });
  } catch (error) {
    handleError(res, error);
  }
});

server.listen(PORT, HOST, () => {
  console.log(`translate-stat-app listening on http://${HOST}:${PORT}`);
  console.log(`dashboard: http://${HOST}:${PORT}/`);
  console.log(`data dir: ${DATA_DIR}`);
  console.log(`timezone: ${TIME_ZONE}, retention: ${RETENTION_DAYS} days`);
});

for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, () => {
    server.close(() => {
      store.close();
      process.exit(0);
    });
  });
}

function setCorsHeaders(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
}

function sendJson(res, statusCode, payload) {
  res.writeHead(statusCode, {
    'Content-Type': 'application/json; charset=utf-8'
  });
  res.end(JSON.stringify(payload));
}

function handleError(res, error) {
  if (error instanceof ValidationError) {
    sendJson(res, error.statusCode || 400, {
      ok: false,
      error: error.message,
      details: error.details || []
    });
    return;
  }

  if (error instanceof SyntaxError) {
    sendJson(res, 400, {
      ok: false,
      error: 'Invalid JSON body.'
    });
    return;
  }

  console.error(error);
  sendJson(res, 500, {
    ok: false,
    error: 'Internal Server Error'
  });
}

async function readJsonBody(req) {
  const contentType = req.headers['content-type'] || '';
  if (!contentType.includes('application/json')) {
    throw new ValidationError('Content-Type must be application/json.');
  }

  const chunks = [];
  for await (const chunk of req) {
    chunks.push(chunk);
  }

  const body = Buffer.concat(chunks).toString('utf8');
  if (body.trim() === '') {
    throw new ValidationError('Request body cannot be empty.');
  }

  return JSON.parse(body);
}

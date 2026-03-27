const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');
const assert = require('node:assert/strict');

const { StatsStore } = require('../src/store');

function createStore(retentionDays = 30) {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'translate-stat-app-'));
  const store = new StatsStore({
    dataDir: tempDir,
    timeZone: 'Asia/Shanghai',
    retentionDays,
    nowProvider: () => new Date('2026-03-27T10:00:00+08:00').getTime()
  });
  store.initialize();
  return { store, tempDir };
}

test('insertEvents writes to daily sqlite files and aggregates correctly', () => {
  const { store, tempDir } = createStore(3650);

  store.insertEvents({
    events: [
      {
        app: 'desktop-app',
        provider: 'openai',
        success: true,
        duration_ms: 120,
        ts: '2026-03-25T09:00:00+08:00',
        app_version: '1.0.0',
        username: 'alice'
      },
      {
        app: 'desktop-app',
        provider: 'openai',
        success: false,
        duration_ms: 380,
        ts: '2026-03-25T10:00:00+08:00',
        app_version: '1.0.0',
        username: 'alice'
      },
      {
        app: 'web-app',
        provider: 'openai',
        success: true,
        duration_ms: 80,
        ts: '2026-03-26T12:00:00+08:00',
        app_version: '1.0.0',
        username: 'alice'
      },
      {
        app: 'web-app',
        provider: 'deepl',
        success: true,
        duration_ms: 90,
        ts: '2026-03-26T13:00:00+08:00',
        app_version: '2.1.0',
        username: 'bob'
      },
      {
        app: 'nat',
        provider: 'openai',
        success: true,
        duration_ms: 999,
        ts: '2026-03-26T14:00:00+08:00',
        app_version: '9.9.9',
        username: 'system'
      }
    ]
  });

  const files = fs.readdirSync(tempDir).filter((name) => name.endsWith('.sqlite')).sort();
  assert.deepEqual(files, [
    'translate-stats-2026-03-25.sqlite',
    'translate-stats-2026-03-26.sqlite'
  ]);

  const dashboard = store.getDashboardData({
    from: '2026-03-25',
    to: '2026-03-26'
  });

  assert.equal(dashboard.summary.total, 4);
  assert.equal(dashboard.summary.success_count, 3);
  assert.equal(dashboard.summary.failure_count, 1);
  assert.equal(dashboard.summary.success_rate, 75);
  assert.equal(dashboard.daily.length, 2);
  assert.deepEqual(dashboard.daily_by_app.map((item) => item.app), ['desktop-app', 'web-app']);
  assert.equal(dashboard.daily_by_app[0].points[0].total, 2);
  assert.equal(dashboard.daily_by_app[0].points[1].total, 0);
  assert.equal(dashboard.daily_by_app[1].points[0].total, 0);
  assert.equal(dashboard.daily_by_app[1].points[1].total, 2);
  assert.equal(dashboard.providers[0].provider, 'openai');
  assert.equal(dashboard.providers[0].total, 3);
  assert.ok(!dashboard.apps.some((item) => item.app === 'nat'));
  assert.ok(!dashboard.users.some((item) => item.app === 'nat'));
  assert.ok(!dashboard.daily_by_app.some((item) => item.app === 'nat'));
  assert.equal(dashboard.users[0].label, 'desktop-app / alice');
  assert.equal(dashboard.users[0].total, 2);
  assert.ok(dashboard.users.some((item) => item.label === 'web-app / alice' && item.total === 1));
  assert.ok(dashboard.versions.some((item) => item.label === 'nat / 9.9.9' && item.avg_duration_ms === 999));
  assert.equal(dashboard.versions[0].label, 'desktop-app / 1.0.0');
  assert.equal(dashboard.versions[0].total, 2);
  assert.ok(dashboard.versions.some((item) => item.label === 'web-app / 1.0.0' && item.total === 1));

  store.close();
});

test('cleanupOldDatabases deletes files older than retention window', () => {
  const { store, tempDir } = createStore(30);

  store.insertEvents([
    {
      app: 'desktop-app',
      provider: 'openai',
      success: true,
      duration_ms: 100,
      ts: '2026-01-01T08:00:00+08:00',
      app_version: '1.0.0',
      username: 'alice'
    },
    {
      app: 'desktop-app',
      provider: 'openai',
      success: true,
      duration_ms: 120,
      ts: '2026-03-27T08:00:00+08:00',
      app_version: '1.0.1',
      username: 'alice'
    }
  ]);

  store.cleanupOldDatabases(new Date('2026-03-27T10:00:00+08:00').getTime());

  const files = fs.readdirSync(tempDir).filter((name) => name.endsWith('.sqlite')).sort();
  assert.deepEqual(files, ['translate-stats-2026-03-27.sqlite']);

  store.close();
});

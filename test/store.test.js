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
  assert.deepEqual(dashboard.provider_hourly.map((item) => item.provider), ['openai', 'deepl']);
  assert.equal(dashboard.provider_hourly[0].points.length, 48);
  assert.ok(dashboard.provider_hourly[0].points.some((point) => point.date === '2026-03-25 09:00' && point.total === 1));
  assert.ok(dashboard.provider_hourly[1].points.some((point) => point.date === '2026-03-26 13:00' && point.total === 1));
  assert.ok(!dashboard.provider_hourly.some((item) => item.provider === 'nat'));
  assert.equal(dashboard.providers[0].provider, 'openai');
  assert.equal(dashboard.providers[0].total, 3);
  assert.equal(dashboard.nat_providers.length, 1);
  assert.equal(dashboard.nat_providers[0].provider, 'openai');
  assert.equal(dashboard.nat_providers[0].total, 1);
  assert.equal(dashboard.nat_providers[0].success_rate, 100);
  assert.equal(dashboard.nat_providers[0].avg_duration_ms, 999);
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

test('date range filters only include rows inside the selected local-day window', () => {
  const { store } = createStore(3650);

  store.insertEvents({
    events: [
      {
        app: 'desktop-app',
        provider: 'openai',
        success: true,
        duration_ms: 50,
        ts: '2026-03-24T23:59:59+08:00',
        app_version: '1.0.0',
        username: 'alice'
      },
      {
        app: 'desktop-app',
        provider: 'openai',
        success: true,
        duration_ms: 60,
        ts: '2026-03-25T00:00:00+08:00',
        app_version: '1.0.0',
        username: 'alice'
      },
      {
        app: 'desktop-app',
        provider: 'openai',
        success: true,
        duration_ms: 70,
        ts: '2026-03-25T23:59:59+08:00',
        app_version: '1.0.0',
        username: 'alice'
      },
      {
        app: 'desktop-app',
        provider: 'openai',
        success: true,
        duration_ms: 80,
        ts: '2026-03-26T00:00:00+08:00',
        app_version: '1.0.0',
        username: 'alice'
      }
    ]
  });

  const dashboard = store.getDashboardData({
    from: '2026-03-25',
    to: '2026-03-25'
  });

  assert.equal(dashboard.summary.total, 2);
  assert.deepEqual(
    dashboard.daily.map((item) => ({ date: item.date, total: item.total })),
    [{ date: '2026-03-25', total: 2 }]
  );

  store.close();
});

test('date range filters still work when a row is stored in an adjacent daily db file', () => {
  const { store } = createStore(3650);

  const misplacedConnection = store.getConnection('2026-03-27');
  misplacedConnection.insert.run(
    'desktop-app',
    'openai',
    1,
    88,
    new Date('2026-03-28T00:10:00+08:00').getTime(),
    '1.0.0',
    'alice'
  );

  const dashboard = store.getDashboardData({
    from: '2026-03-28',
    to: '2026-03-28'
  });

  assert.equal(dashboard.summary.total, 1);
  assert.deepEqual(
    dashboard.daily.map((item) => ({ date: item.date, total: item.total })),
    [{ date: '2026-03-28', total: 1 }]
  );

  store.close();
});

test('provider hourly series only keeps the latest two local days inside the selected range', () => {
  const { store } = createStore(3650);

  store.insertEvents({
    events: [
      {
        app: 'desktop-app',
        provider: 'openai',
        success: true,
        duration_ms: 50,
        ts: '2026-03-24T10:00:00+08:00',
        app_version: '1.0.0',
        username: 'alice'
      },
      {
        app: 'desktop-app',
        provider: 'openai',
        success: true,
        duration_ms: 60,
        ts: '2026-03-25T11:00:00+08:00',
        app_version: '1.0.0',
        username: 'alice'
      },
      {
        app: 'desktop-app',
        provider: 'openai',
        success: false,
        duration_ms: 70,
        ts: '2026-03-26T12:00:00+08:00',
        app_version: '1.0.0',
        username: 'alice'
      }
    ]
  });

  const dashboard = store.getDashboardData({
    from: '2026-03-24',
    to: '2026-03-26'
  });

  assert.equal(dashboard.provider_hourly.length, 1);
  assert.equal(dashboard.provider_hourly[0].points.length, 48);
  assert.ok(dashboard.provider_hourly[0].points.every((point) => !point.date.startsWith('2026-03-24')));
  assert.ok(dashboard.provider_hourly[0].points.some((point) => point.date === '2026-03-25 11:00' && point.total === 1));
  assert.ok(dashboard.provider_hourly[0].points.some((point) => point.date === '2026-03-26 12:00' && point.total === 1));

  store.close();
});

test('default date range uses the latest seven local days within retention', () => {
  const { store } = createStore(30);

  const options = store.getOptions();
  const dashboard = store.getDashboardData();

  assert.equal(options.range.from, '2026-03-21');
  assert.equal(options.range.to, '2026-03-27');
  assert.equal(options.range.defaultFrom, '2026-03-21');
  assert.equal(dashboard.range.from, '2026-03-21');
  assert.equal(dashboard.range.to, '2026-03-27');

  store.close();
});

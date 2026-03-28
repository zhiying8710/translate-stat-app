const fs = require('node:fs');
const path = require('node:path');
const { DatabaseSync } = require('./sqlite-driver');

const {
  dateKeyFromTimestamp,
  enumerateDateKeys,
  getRangeTimestamps,
  resolveDateRange,
  shiftDateKey
} = require('./date-utils');

const DATABASE_PREFIX = 'translate-stats-';
const DATABASE_SUFFIX = '.sqlite';

class ValidationError extends Error {
  constructor(message, details = []) {
    super(message);
    this.name = 'ValidationError';
    this.statusCode = 400;
    this.details = details;
  }
}

class StatsStore {
  constructor({ dataDir, timeZone, retentionDays, nowProvider = () => Date.now() }) {
    this.dataDir = dataDir;
    this.timeZone = timeZone;
    this.retentionDays = retentionDays;
    this.nowProvider = nowProvider;
    this.connectionCache = new Map();
  }

  initialize() {
    fs.mkdirSync(this.dataDir, { recursive: true });
    this.cleanupOldDatabases();
  }

  close() {
    for (const connection of this.connectionCache.values()) {
      connection.db.close();
    }
    this.connectionCache.clear();
  }

  insertEvents(payload) {
    const events = this.normalizePayload(payload);
    this.cleanupOldDatabases();

    const grouped = new Map();
    for (const event of events) {
      const dateKey = dateKeyFromTimestamp(event.ts, this.timeZone);
      if (!grouped.has(dateKey)) {
        grouped.set(dateKey, []);
      }
      grouped.get(dateKey).push(event);
    }

    for (const [dateKey, rows] of grouped.entries()) {
      const connection = this.getConnection(dateKey);
      connection.db.exec('BEGIN');
      try {
        for (const row of rows) {
          connection.insert.run(
            row.app,
            row.provider,
            row.success ? 1 : 0,
            row.duration_ms,
            row.ts,
            row.app_version,
            row.username
          );
        }
        connection.db.exec('COMMIT');
      } catch (error) {
        connection.db.exec('ROLLBACK');
        throw error;
      }
    }

    this.cleanupOldDatabases();

    return {
      inserted: events.length,
      days: Array.from(grouped.keys()).sort(),
      retained_days: this.retentionDays
    };
  }

  getOptions(input = {}) {
    this.cleanupOldDatabases();

    const range = resolveDateRange({
      from: input.from,
      to: input.to,
      timeZone: this.timeZone,
      retentionDays: this.retentionDays
    });

    const rows = this.readRows({}, range);
    const apps = new Set();
    const providers = new Set();
    const usernames = new Set();
    const appVersions = new Set();

    for (const row of rows) {
      apps.add(row.app);
      providers.add(row.provider);
      usernames.add(row.username);
      appVersions.add(row.app_version);
    }

    const availableDates = this.listDateKeys().filter((dateKey) => dateKey >= range.from && dateKey <= range.to);

    return {
      range,
      available_dates: availableDates,
      apps: sortValues(apps),
      providers: sortValues(providers),
      usernames: sortValues(usernames),
      app_versions: sortValues(appVersions)
    };
  }

  getDashboardData(input = {}) {
    this.cleanupOldDatabases();

    const range = resolveDateRange({
      from: input.from,
      to: input.to,
      timeZone: this.timeZone,
      retentionDays: this.retentionDays
    });

    const filters = {
      app: normalizeOptionalString(input.app),
      provider: normalizeOptionalString(input.provider),
      username: normalizeOptionalString(input.username),
      app_version: normalizeOptionalString(input.app_version),
      success: parseOptionalSuccess(input.success)
    };

    const rows = this.readRows(filters, range);
    const metricRows = excludeNatFromStats(rows);
    const natRows = rows.filter((row) => isNatApp(row.app));
    const daily = buildDailySeries(metricRows, range, this.timeZone);
    const daily_by_app = buildDailySeriesByApp(metricRows, range, this.timeZone);
    const providers = buildAggregate(metricRows, 'provider', 'provider');
    const nat_providers = buildAggregate(natRows, 'provider', 'provider');
    const apps = buildAggregate(metricRows, 'app', 'app');
    const users = buildCompositeAggregate(metricRows, {
      outputKeyName: 'app_username',
      identity: (row) => `${row.app}::${row.username}`,
      fields: (row) => ({
        app: row.app,
        username: row.username,
        app_username: `${row.app} / ${row.username}`,
        label: `${row.app} / ${row.username}`
      })
    });
    const versions = buildCompositeAggregate(rows, {
      outputKeyName: 'app_app_version',
      identity: (row) => `${row.app}::${row.app_version}`,
      fields: (row) => ({
        app: row.app,
        app_version: row.app_version,
        app_app_version: `${row.app} / ${row.app_version}`,
        label: `${row.app} / ${row.app_version}`
      })
    });
    const summary = buildSummary(metricRows);

    return {
      range,
      filters,
      summary,
      daily,
      daily_by_app,
      providers,
      nat_providers,
      apps,
      users,
      versions
    };
  }

  cleanupOldDatabases(now = this.nowProvider()) {
    const today = dateKeyFromTimestamp(now, this.timeZone);
    const cutoff = shiftDateKey(today, -(this.retentionDays - 1));

    for (const dateKey of this.listDateKeys()) {
      if (dateKey < cutoff) {
        this.closeConnection(dateKey);
        const filePath = this.getDatabasePath(dateKey);
        if (fs.existsSync(filePath)) {
          fs.unlinkSync(filePath);
        }
      }
    }
  }

  normalizePayload(payload) {
    const rawEvents = Array.isArray(payload)
      ? payload
      : Array.isArray(payload?.events)
        ? payload.events
        : payload
          ? [payload]
          : [];

    if (!Array.isArray(rawEvents) || rawEvents.length === 0) {
      throw new ValidationError('Request body must contain one event object or an `events` array.');
    }

    return rawEvents.map((row, index) => normalizeEvent(row, index));
  }

  readRows(filters, range) {
    const rows = [];
    const { startTs, endTsExclusive } = getRangeTimestamps(range.from, range.to, this.timeZone);

    // Query all retained daily databases and rely on ts bounds for correctness.
    // This keeps range queries accurate even if some historical rows were written
    // into an adjacent day's file because of older partitioning logic.
    for (const dateKey of this.listDateKeys()) {
      const filePath = this.getDatabasePath(dateKey);
      if (!fs.existsSync(filePath)) {
        continue;
      }

      const connection = this.getConnection(dateKey);
      const { whereClause, params } = buildWhereClause(filters, {
        startTs,
        endTsExclusive
      });
      const statement = connection.db.prepare(`
        SELECT
          app,
          provider,
          success,
          duration_ms,
          ts,
          app_version,
          username
        FROM translation_stats
        ${whereClause}
        ORDER BY ts ASC
      `);

      const dayRows = statement.all(...params);
      for (const row of dayRows) {
        rows.push({
          ...row,
          success: Boolean(row.success)
        });
      }
    }

    return rows;
  }

  listDateKeys() {
    if (!fs.existsSync(this.dataDir)) {
      return [];
    }

    return fs.readdirSync(this.dataDir)
      .map((fileName) => {
        const match = fileName.match(/^translate-stats-(\d{4}-\d{2}-\d{2})\.sqlite$/);
        return match ? match[1] : null;
      })
      .filter(Boolean)
      .sort();
  }

  getDatabasePath(dateKey) {
    return path.join(this.dataDir, `${DATABASE_PREFIX}${dateKey}${DATABASE_SUFFIX}`);
  }

  getConnection(dateKey) {
    if (this.connectionCache.has(dateKey)) {
      return this.connectionCache.get(dateKey);
    }

    const filePath = this.getDatabasePath(dateKey);
    const db = new DatabaseSync(filePath);
    db.exec(`
      PRAGMA synchronous = NORMAL;
      PRAGMA temp_store = MEMORY;

      CREATE TABLE IF NOT EXISTS translation_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        app TEXT NOT NULL,
        provider TEXT NOT NULL,
        success INTEGER NOT NULL CHECK (success IN (0, 1)),
        duration_ms INTEGER NOT NULL CHECK (duration_ms >= 0),
        ts INTEGER NOT NULL,
        app_version TEXT NOT NULL,
        username TEXT NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_translation_stats_ts ON translation_stats (ts);
      CREATE INDEX IF NOT EXISTS idx_translation_stats_app ON translation_stats (app);
      CREATE INDEX IF NOT EXISTS idx_translation_stats_provider ON translation_stats (provider);
      CREATE INDEX IF NOT EXISTS idx_translation_stats_username ON translation_stats (username);
      CREATE INDEX IF NOT EXISTS idx_translation_stats_app_version ON translation_stats (app_version);
      CREATE INDEX IF NOT EXISTS idx_translation_stats_success ON translation_stats (success);
    `);

    const connection = {
      db,
      insert: db.prepare(`
        INSERT INTO translation_stats (
          app,
          provider,
          success,
          duration_ms,
          ts,
          app_version,
          username
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
      `)
    };

    this.connectionCache.set(dateKey, connection);
    return connection;
  }

  closeConnection(dateKey) {
    const connection = this.connectionCache.get(dateKey);
    if (!connection) {
      return;
    }

    connection.db.close();
    this.connectionCache.delete(dateKey);
  }
}

function normalizeEvent(input, index) {
  if (!input || typeof input !== 'object' || Array.isArray(input)) {
    throw new ValidationError(`Event at index ${index} must be an object.`);
  }

  return {
    app: requireNonEmptyString(input.app, `events[${index}].app`),
    provider: requireNonEmptyString(input.provider, `events[${index}].provider`),
    success: parseRequiredSuccess(input.success, `events[${index}].success`),
    duration_ms: parseDuration(input.duration_ms, `events[${index}].duration_ms`),
    ts: parseTimestamp(input.ts, `events[${index}].ts`),
    app_version: requireNonEmptyString(input.app_version, `events[${index}].app_version`),
    username: requireNonEmptyString(input.username, `events[${index}].username`)
  };
}

function requireNonEmptyString(value, fieldName) {
  if (typeof value !== 'string' || value.trim() === '') {
    throw new ValidationError(`${fieldName} must be a non-empty string.`);
  }
  return value.trim();
}

function normalizeOptionalString(value) {
  if (typeof value !== 'string') {
    return undefined;
  }

  const normalized = value.trim();
  return normalized === '' ? undefined : normalized;
}

function parseRequiredSuccess(value, fieldName) {
  const parsed = parseOptionalSuccess(value);
  if (typeof parsed !== 'boolean') {
    throw new ValidationError(`${fieldName} must be a boolean, 0/1, or true/false.`);
  }
  return parsed;
}

function parseOptionalSuccess(value) {
  if (value === undefined || value === null || value === '' || value === 'all') {
    return undefined;
  }

  if (typeof value === 'boolean') {
    return value;
  }

  if (typeof value === 'number') {
    if (value === 1) {
      return true;
    }
    if (value === 0) {
      return false;
    }
  }

  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (normalized === 'true' || normalized === '1') {
      return true;
    }
    if (normalized === 'false' || normalized === '0') {
      return false;
    }
  }

  return undefined;
}

function parseDuration(value, fieldName) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric < 0) {
    throw new ValidationError(`${fieldName} must be a non-negative number.`);
  }
  return Math.round(numeric);
}

function parseTimestamp(value, fieldName) {
  if (value === undefined || value === null || value === '') {
    throw new ValidationError(`${fieldName} is required.`);
  }

  let numericTimestamp;
  if (typeof value === 'string' && value.trim() !== '' && !/^\d+(\.\d+)?$/.test(value.trim())) {
    numericTimestamp = new Date(value).getTime();
  } else {
    numericTimestamp = Number(value);
    if (numericTimestamp < 1e12) {
      numericTimestamp *= 1000;
    }
  }

  if (!Number.isFinite(numericTimestamp)) {
    throw new ValidationError(`${fieldName} must be a valid Unix timestamp or ISO datetime.`);
  }

  return Math.round(numericTimestamp);
}

function excludeNatFromStats(rows) {
  return rows.filter((row) => !isNatApp(row.app));
}

function isNatApp(app) {
  return String(app).trim().toLowerCase() === 'nat';
}

function buildWhereClause(filters, rangeFilter = {}) {
  const clauses = ['ts >= ?', 'ts < ?'];
  const params = [rangeFilter.startTs, rangeFilter.endTsExclusive];

  if (filters.app) {
    clauses.push('app = ?');
    params.push(filters.app);
  }

  if (filters.provider) {
    clauses.push('provider = ?');
    params.push(filters.provider);
  }

  if (filters.username) {
    clauses.push('username = ?');
    params.push(filters.username);
  }

  if (filters.app_version) {
    clauses.push('app_version = ?');
    params.push(filters.app_version);
  }

  if (typeof filters.success === 'boolean') {
    clauses.push('success = ?');
    params.push(filters.success ? 1 : 0);
  }

  return {
    whereClause: `WHERE ${clauses.join(' AND ')}`,
    params
  };
}

function buildSummary(rows) {
  const total = rows.length;
  const successCount = rows.filter((row) => row.success).length;
  const failureCount = total - successCount;
  const totalDuration = rows.reduce((sum, row) => sum + row.duration_ms, 0);

  return {
    total,
    success_count: successCount,
    failure_count: failureCount,
    success_rate: total === 0 ? 0 : Number(((successCount / total) * 100).toFixed(2)),
    avg_duration_ms: total === 0 ? 0 : Number((totalDuration / total).toFixed(2)),
    unique_apps: new Set(rows.map((row) => row.app)).size,
    unique_providers: new Set(rows.map((row) => row.provider)).size,
    unique_users: new Set(rows.map((row) => row.username)).size,
    unique_versions: new Set(rows.map((row) => row.app_version)).size
  };
}

function buildDailySeries(rows, range, timeZone) {
  const series = new Map();

  for (const dateKey of enumerateDateKeys(range.from, range.to)) {
    series.set(dateKey, createEmptyAggregate(dateKey, 'date'));
  }

  for (const row of rows) {
    const dateKey = dateKeyFromTimestamp(row.ts, timeZone);
    const aggregate = series.get(dateKey);
    if (aggregate) {
      updateAggregate(aggregate, row);
    }
  }

  return Array.from(series.values()).map(finalizeAggregate);
}

function buildDailySeriesByApp(rows, range, timeZone) {
  const groups = new Map();
  const dateKeys = enumerateDateKeys(range.from, range.to);

  for (const row of rows) {
    if (!groups.has(row.app)) {
      groups.set(row.app, createSeriesTemplate(dateKeys));
    }

    const dateKey = dateKeyFromTimestamp(row.ts, timeZone);
    const aggregate = groups.get(row.app).get(dateKey);
    if (aggregate) {
      updateAggregate(aggregate, row);
    }
  }

  return Array.from(groups.entries())
    .map(([app, series]) => {
      const points = Array.from(series.values()).map(finalizeAggregate);
      return {
        app,
        label: app,
        total: points.reduce((sum, point) => sum + point.total, 0),
        points
      };
    })
    .sort((left, right) => {
      if (right.total !== left.total) {
        return right.total - left.total;
      }
      return left.app.localeCompare(right.app, 'zh-Hans-CN');
    });
}

function buildAggregate(rows, fieldName, outputKeyName) {
  const groups = new Map();

  for (const row of rows) {
    const groupKey = row[fieldName];
    if (!groups.has(groupKey)) {
      groups.set(groupKey, createEmptyAggregate(groupKey, outputKeyName));
    }

    updateAggregate(groups.get(groupKey), row);
  }

  return Array.from(groups.values())
    .map(finalizeAggregate)
    .sort((left, right) => {
      if (right.total !== left.total) {
        return right.total - left.total;
      }
      return String(left[outputKeyName]).localeCompare(String(right[outputKeyName]), 'zh-Hans-CN');
    });
}

function buildCompositeAggregate(rows, config) {
  const groups = new Map();

  for (const row of rows) {
    const identity = config.identity(row);
    if (!groups.has(identity)) {
      groups.set(identity, createAggregate(config.fields(row)));
    }

    updateAggregate(groups.get(identity), row);
  }

  return Array.from(groups.values())
    .map(finalizeAggregate)
    .sort((left, right) => {
      if (right.total !== left.total) {
        return right.total - left.total;
      }
      return String(left[config.outputKeyName]).localeCompare(String(right[config.outputKeyName]), 'zh-Hans-CN');
    });
}

function createSeriesTemplate(dateKeys) {
  const series = new Map();

  for (const dateKey of dateKeys) {
    series.set(dateKey, createEmptyAggregate(dateKey, 'date'));
  }

  return series;
}

function createEmptyAggregate(keyValue, keyName) {
  return createAggregate({
    [keyName]: keyValue
  });
}

function createAggregate(fields = {}) {
  return {
    ...fields,
    total: 0,
    success_count: 0,
    failure_count: 0,
    duration_total: 0
  };
}

function updateAggregate(aggregate, row) {
  aggregate.total += 1;
  aggregate.duration_total += row.duration_ms;

  if (row.success) {
    aggregate.success_count += 1;
  } else {
    aggregate.failure_count += 1;
  }
}

function finalizeAggregate(aggregate) {
  return {
    ...aggregate,
    success_rate: aggregate.total === 0 ? 0 : Number(((aggregate.success_count / aggregate.total) * 100).toFixed(2)),
    avg_duration_ms: aggregate.total === 0 ? 0 : Number((aggregate.duration_total / aggregate.total).toFixed(2))
  };
}

function sortValues(values) {
  return Array.from(values).sort((left, right) => left.localeCompare(right, 'zh-Hans-CN'));
}

module.exports = {
  StatsStore,
  ValidationError
};

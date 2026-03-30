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
const hourFormatterCache = new Map();

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
    this.dateKeysCache = null;
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
      retentionDays: this.retentionDays,
      now: this.nowProvider()
    });

    const apps = new Set();
    const providers = new Set();
    const usernames = new Set();
    const appVersions = new Set();

    this.scanRows(range, {}, (row) => {
      apps.add(row.app);
      providers.add(row.provider);
      usernames.add(row.username);
      appVersions.add(row.app_version);
    });

    const availableDates = this.listDateKeysForRange(range);

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
    const { range, filters } = resolveDashboardQuery(this, input);
    const dateKeys = enumerateDateKeys(range.from, range.to);
    const recentFrom = range.from > shiftDateKey(range.to, -1)
      ? range.from
      : shiftDateKey(range.to, -1);
    const hourKeys = enumerateHourKeys(recentFrom, range.to);
    const dailySeries = createSeriesTemplate(dateKeys);
    const dailyByApp = new Map();
    const providerHourly = new Map();
    const providers = new Map();
    const natProviders = new Map();
    const apps = new Map();
    const users = new Map();
    const versions = new Map();
    const summaryAggregate = createAggregate();
    const summaryApps = new Set();
    const summaryProviders = new Set();
    const summaryUsers = new Set();
    const summaryVersions = new Set();
    const optionApps = new Set();
    const optionProviders = new Set();
    const optionUsers = new Set();
    const optionVersions = new Set();

    this.scanRows(range, {}, (row) => {
      optionApps.add(row.app);
      optionProviders.add(row.provider);
      optionUsers.add(row.username);
      optionVersions.add(row.app_version);

      if (!rowMatchesFilters(row, filters)) {
        return;
      }

      updateMapAggregate(versions, `${row.app}::${row.app_version}`, {
        app: row.app,
        app_version: row.app_version,
        app_app_version: `${row.app} / ${row.app_version}`,
        label: `${row.app} / ${row.app_version}`
      }, row);

      if (isNatApp(row.app)) {
        updateMapAggregate(natProviders, row.provider, { provider: row.provider }, row);
        return;
      }

      updateAggregate(summaryAggregate, row);
      summaryApps.add(row.app);
      summaryProviders.add(row.provider);
      summaryUsers.add(row.username);
      summaryVersions.add(row.app_version);

      const dateKey = dateKeyFromTimestamp(row.ts, this.timeZone);
      updateExistingAggregate(dailySeries, dateKey, row);
      updateSeriesAggregate(dailyByApp, row.app, dateKeys, dateKey, row);

      if (dateKey >= recentFrom && dateKey <= range.to) {
        const hourKey = hourKeyFromTimestamp(row.ts, this.timeZone);
        updateSeriesAggregate(providerHourly, row.provider, hourKeys, hourKey, row);
      }

      updateMapAggregate(providers, row.provider, { provider: row.provider }, row);
      updateMapAggregate(apps, row.app, { app: row.app }, row);
      updateMapAggregate(users, `${row.app}::${row.username}`, {
        app: row.app,
        username: row.username,
        app_username: `${row.app} / ${row.username}`,
        label: `${row.app} / ${row.username}`
      }, row);
    });

    return {
      range,
      filters,
      options: {
        available_dates: this.listDateKeysForRange(range),
        apps: sortValues(optionApps),
        providers: sortValues(optionProviders),
        usernames: sortValues(optionUsers),
        app_versions: sortValues(optionVersions)
      },
      summary: finalizeSummaryAggregate(summaryAggregate, {
        apps: summaryApps,
        providers: summaryProviders,
        users: summaryUsers,
        versions: summaryVersions
      }),
      daily: finalizeSeries(dailySeries),
      daily_by_app: finalizeSeriesGroups(dailyByApp, 'app'),
      provider_hourly: finalizeSeriesGroups(providerHourly, 'provider'),
      providers: finalizeAggregateGroups(providers, 'provider'),
      nat_providers: finalizeAggregateGroups(natProviders, 'provider'),
      apps: finalizeAggregateGroups(apps, 'app'),
      users: finalizeAggregateGroups(users, 'app_username'),
      versions: finalizeAggregateGroups(versions, 'app_app_version')
    };
  }

  getDashboardSummary(input = {}) {
    const { range, filters, metricRows } = createDashboardContext(this, input);

    return {
      range,
      filters,
      summary: buildSummary(metricRows)
    };
  }

  getDashboardTrends(input = {}) {
    const { range, filters, metricRows } = createDashboardContext(this, input);

    return {
      range,
      filters,
      daily_by_app: buildDailySeriesByApp(metricRows, range, this.timeZone)
    };
  }

  getDashboardProviders(input = {}) {
    const { range, filters, metricRows } = createDashboardContext(this, input);

    return {
      range,
      filters,
      provider_hourly: buildHourlySeriesByProvider(metricRows, range, this.timeZone),
      providers: buildAggregate(metricRows, 'provider', 'provider')
    };
  }

  getDashboardApps(input = {}) {
    const { range, filters, rows, metricRows } = createDashboardContext(this, input);

    return {
      range,
      filters,
      apps: buildAggregate(metricRows, 'app', 'app'),
      users: buildCompositeAggregate(metricRows, {
        outputKeyName: 'app_username',
        identity: (row) => `${row.app}::${row.username}`,
        fields: (row) => ({
          app: row.app,
          username: row.username,
          app_username: `${row.app} / ${row.username}`,
          label: `${row.app} / ${row.username}`
        })
      }),
      versions: buildCompositeAggregate(rows, {
        outputKeyName: 'app_app_version',
        identity: (row) => `${row.app}::${row.app_version}`,
        fields: (row) => ({
          app: row.app,
          app_version: row.app_version,
          app_app_version: `${row.app} / ${row.app_version}`,
          label: `${row.app} / ${row.app_version}`
        })
      })
    };
  }

  getDashboardNatProviders(input = {}) {
    const { range, filters, natRows } = createDashboardContext(this, input);

    return {
      range,
      filters,
      nat_providers: buildAggregate(natRows, 'provider', 'provider')
    };
  }

  cleanupOldDatabases(now = this.nowProvider()) {
    const today = dateKeyFromTimestamp(now, this.timeZone);
    const cutoff = shiftDateKey(today, -(this.retentionDays - 1));
    let removed = false;

    for (const dateKey of this.listDateKeys()) {
      if (dateKey < cutoff) {
        this.closeConnection(dateKey);
        const filePath = this.getDatabasePath(dateKey);
        if (fs.existsSync(filePath)) {
          fs.unlinkSync(filePath);
          removed = true;
        }
      }
    }

    if (removed) {
      this.invalidateDateKeysCache();
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
    this.scanRows(range, filters, (row) => {
      rows.push(row);
    });

    return rows;
  }

  listDateKeys() {
    if (!fs.existsSync(this.dataDir)) {
      return [];
    }

    if (this.dateKeysCache) {
      return this.dateKeysCache;
    }

    this.dateKeysCache = fs.readdirSync(this.dataDir)
      .map((fileName) => {
        const match = fileName.match(/^translate-stats-(\d{4}-\d{2}-\d{2})\.sqlite$/);
        return match ? match[1] : null;
      })
      .filter(Boolean)
      .sort();

    return this.dateKeysCache;
  }

  listDateKeysForRange(range, spilloverDays = 0) {
    const from = spilloverDays > 0 ? shiftDateKey(range.from, -spilloverDays) : range.from;
    const to = spilloverDays > 0 ? shiftDateKey(range.to, spilloverDays) : range.to;

    return this.listDateKeys().filter((dateKey) => dateKey >= from && dateKey <= to);
  }

  getDatabasePath(dateKey) {
    return path.join(this.dataDir, `${DATABASE_PREFIX}${dateKey}${DATABASE_SUFFIX}`);
  }

  getConnection(dateKey) {
    if (this.connectionCache.has(dateKey)) {
      return this.connectionCache.get(dateKey);
    }

    const filePath = this.getDatabasePath(dateKey);
    const existed = fs.existsSync(filePath);
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
      readStatements: new Map(),
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

    if (!existed) {
      this.invalidateDateKeysCache();
    }

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

  invalidateDateKeysCache() {
    this.dateKeysCache = null;
  }

  scanRows(range, filters, onRow) {
    const { startTs, endTsExclusive } = getRangeTimestamps(range.from, range.to, this.timeZone);
    const { whereClause, params, signature } = buildWhereClause(filters, {
      startTs,
      endTsExclusive
    });

    // Scan only the requested day range and one adjacent day on each side.
    // Older partitioning logic could spill rows into a neighboring file.
    for (const dateKey of this.listDateKeysForRange(range, 1)) {
      const filePath = this.getDatabasePath(dateKey);
      if (!fs.existsSync(filePath)) {
        continue;
      }

      const connection = this.getConnection(dateKey);
      const statement = getReadStatement(connection, signature, whereClause);
      for (const row of statement.iterate(...params)) {
        onRow(normalizeReadRow(row));
      }
    }
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

function createDashboardContext(store, input = {}) {
  store.cleanupOldDatabases();
  const { range, filters } = resolveDashboardQuery(store, input);

  const rows = store.readRows(filters, range);

  return {
    range,
    filters,
    rows,
    metricRows: excludeNatFromStats(rows),
    natRows: rows.filter((row) => isNatApp(row.app))
  };
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
  const signatureParts = [];

  if (filters.app) {
    clauses.push('app = ?');
    params.push(filters.app);
    signatureParts.push('app');
  }

  if (filters.provider) {
    clauses.push('provider = ?');
    params.push(filters.provider);
    signatureParts.push('provider');
  }

  if (filters.username) {
    clauses.push('username = ?');
    params.push(filters.username);
    signatureParts.push('username');
  }

  if (filters.app_version) {
    clauses.push('app_version = ?');
    params.push(filters.app_version);
    signatureParts.push('app_version');
  }

  if (typeof filters.success === 'boolean') {
    clauses.push('success = ?');
    params.push(filters.success ? 1 : 0);
    signatureParts.push('success');
  }

  return {
    signature: signatureParts.length === 0 ? 'ts' : `ts:${signatureParts.join(',')}`,
    whereClause: `WHERE ${clauses.join(' AND ')}`,
    params
  };
}

function buildSummary(rows) {
  const aggregate = createAggregate();
  const apps = new Set();
  const providers = new Set();
  const users = new Set();
  const versions = new Set();

  for (const row of rows) {
    updateAggregate(aggregate, row);
    apps.add(row.app);
    providers.add(row.provider);
    users.add(row.username);
    versions.add(row.app_version);
  }

  return finalizeSummaryAggregate(aggregate, {
    apps,
    providers,
    users,
    versions
  });
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

function buildHourlySeriesByProvider(rows, range, timeZone) {
  const recentFrom = range.from > shiftDateKey(range.to, -1)
    ? range.from
    : shiftDateKey(range.to, -1);
  const hourKeys = enumerateHourKeys(recentFrom, range.to);
  const groups = new Map();

  for (const row of rows) {
    const dateKey = dateKeyFromTimestamp(row.ts, timeZone);
    if (dateKey < recentFrom || dateKey > range.to) {
      continue;
    }

    if (!groups.has(row.provider)) {
      groups.set(row.provider, createSeriesTemplate(hourKeys));
    }

    const hourKey = hourKeyFromTimestamp(row.ts, timeZone);
    const aggregate = groups.get(row.provider).get(hourKey);
    if (aggregate) {
      updateAggregate(aggregate, row);
    }
  }

  return Array.from(groups.entries())
    .map(([provider, series]) => {
      const points = Array.from(series.values()).map(finalizeAggregate);
      return {
        provider,
        label: provider,
        total: points.reduce((sum, point) => sum + point.total, 0),
        points
      };
    })
    .sort((left, right) => {
      if (right.total !== left.total) {
        return right.total - left.total;
      }
      return left.provider.localeCompare(right.provider, 'zh-Hans-CN');
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

function enumerateHourKeys(from, to) {
  const hourKeys = [];

  for (const dateKey of enumerateDateKeys(from, to)) {
    for (let hour = 0; hour < 24; hour += 1) {
      hourKeys.push(`${dateKey} ${String(hour).padStart(2, '0')}:00`);
    }
  }

  return hourKeys;
}

function hourKeyFromTimestamp(timestamp, timeZone) {
  const date = new Date(timestamp);
  const formatter = getHourFormatter(timeZone);
  const parts = formatter.formatToParts(date);
  const mapped = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${mapped.year}-${mapped.month}-${mapped.day} ${mapped.hour}:00`;
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

function resolveDashboardQuery(store, input = {}) {
  const range = resolveDateRange({
    from: input.from,
    to: input.to,
    timeZone: store.timeZone,
    retentionDays: store.retentionDays,
    now: store.nowProvider()
  });

  return {
    range,
    filters: {
      app: normalizeOptionalString(input.app),
      provider: normalizeOptionalString(input.provider),
      username: normalizeOptionalString(input.username),
      app_version: normalizeOptionalString(input.app_version),
      success: parseOptionalSuccess(input.success)
    }
  };
}

function getReadStatement(connection, signature, whereClause) {
  const cacheKey = `read:${signature}`;
  if (!connection.readStatements.has(cacheKey)) {
    connection.readStatements.set(cacheKey, connection.db.prepare(`
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
    `));
  }

  return connection.readStatements.get(cacheKey);
}

function normalizeReadRow(row) {
  return {
    ...row,
    success: Boolean(row.success)
  };
}

function rowMatchesFilters(row, filters) {
  if (filters.app && row.app !== filters.app) {
    return false;
  }

  if (filters.provider && row.provider !== filters.provider) {
    return false;
  }

  if (filters.username && row.username !== filters.username) {
    return false;
  }

  if (filters.app_version && row.app_version !== filters.app_version) {
    return false;
  }

  if (typeof filters.success === 'boolean' && row.success !== filters.success) {
    return false;
  }

  return true;
}

function updateMapAggregate(groups, key, fields, row) {
  if (!groups.has(key)) {
    groups.set(key, createAggregate(fields));
  }

  updateAggregate(groups.get(key), row);
}

function updateExistingAggregate(series, key, row) {
  const aggregate = series.get(key);
  if (aggregate) {
    updateAggregate(aggregate, row);
  }
}

function updateSeriesAggregate(groups, groupKey, templateKeys, pointKey, row) {
  if (!groups.has(groupKey)) {
    groups.set(groupKey, createSeriesTemplate(templateKeys));
  }

  updateExistingAggregate(groups.get(groupKey), pointKey, row);
}

function finalizeSummaryAggregate(aggregate, uniqueGroups) {
  const finalized = finalizeAggregate(aggregate);

  return {
    total: finalized.total,
    success_count: finalized.success_count,
    failure_count: finalized.failure_count,
    success_rate: finalized.success_rate,
    avg_duration_ms: finalized.avg_duration_ms,
    unique_apps: uniqueGroups.apps.size,
    unique_providers: uniqueGroups.providers.size,
    unique_users: uniqueGroups.users.size,
    unique_versions: uniqueGroups.versions.size
  };
}

function finalizeSeries(series) {
  return Array.from(series.values()).map(finalizeAggregate);
}

function finalizeSeriesGroups(groups, outputKeyName) {
  return Array.from(groups.entries())
    .map(([groupKey, series]) => {
      const points = finalizeSeries(series);
      return {
        [outputKeyName]: groupKey,
        label: groupKey,
        total: points.reduce((sum, point) => sum + point.total, 0),
        points
      };
    })
    .sort((left, right) => compareAggregateOutputs(left, right, outputKeyName));
}

function finalizeAggregateGroups(groups, outputKeyName) {
  return Array.from(groups.values())
    .map(finalizeAggregate)
    .sort((left, right) => compareAggregateOutputs(left, right, outputKeyName));
}

function compareAggregateOutputs(left, right, outputKeyName) {
  if (right.total !== left.total) {
    return right.total - left.total;
  }

  return String(left[outputKeyName]).localeCompare(String(right[outputKeyName]), 'zh-Hans-CN');
}

function getHourFormatter(timeZone) {
  if (!hourFormatterCache.has(timeZone)) {
    hourFormatterCache.set(timeZone, new Intl.DateTimeFormat('en-CA', {
      timeZone,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      hourCycle: 'h23'
    }));
  }

  return hourFormatterCache.get(timeZone);
}

function sortValues(values) {
  return Array.from(values).sort((left, right) => left.localeCompare(right, 'zh-Hans-CN'));
}

module.exports = {
  StatsStore,
  ValidationError
};

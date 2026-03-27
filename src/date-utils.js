const DATE_KEY_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

function isValidDateKey(value) {
  return typeof value === 'string' && DATE_KEY_PATTERN.test(value);
}

function parseDateKey(dateKey) {
  if (!isValidDateKey(dateKey)) {
    throw new Error(`Invalid date key: ${dateKey}`);
  }

  const [year, month, day] = dateKey.split('-').map(Number);
  return new Date(Date.UTC(year, month - 1, day));
}

function formatDateKey(date) {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function shiftDateKey(dateKey, offsetDays) {
  const date = parseDateKey(dateKey);
  date.setUTCDate(date.getUTCDate() + offsetDays);
  return formatDateKey(date);
}

function enumerateDateKeys(from, to) {
  const start = parseDateKey(from);
  const end = parseDateKey(to);
  const result = [];

  if (start > end) {
    return result;
  }

  const cursor = new Date(start);
  while (cursor <= end) {
    result.push(formatDateKey(cursor));
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }

  return result;
}

function dateKeyFromTimestamp(timestamp, timeZone) {
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) {
    throw new Error(`Invalid timestamp: ${timestamp}`);
  }

  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  });

  const parts = formatter.formatToParts(date);
  const mapped = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${mapped.year}-${mapped.month}-${mapped.day}`;
}

function resolveDateRange({ from, to, timeZone, retentionDays, now = Date.now() }) {
  const today = dateKeyFromTimestamp(now, timeZone);
  const defaultFrom = shiftDateKey(today, -(retentionDays - 1));
  const resolvedFrom = from || defaultFrom;
  const resolvedTo = to || today;

  if (!isValidDateKey(resolvedFrom) || !isValidDateKey(resolvedTo)) {
    throw new Error('Date filters must use YYYY-MM-DD format.');
  }

  if (resolvedFrom > resolvedTo) {
    throw new Error('`from` cannot be greater than `to`.');
  }

  return {
    from: resolvedFrom,
    to: resolvedTo,
    defaultFrom,
    today
  };
}

module.exports = {
  dateKeyFromTimestamp,
  enumerateDateKeys,
  formatDateKey,
  isValidDateKey,
  resolveDateRange,
  shiftDateKey
};

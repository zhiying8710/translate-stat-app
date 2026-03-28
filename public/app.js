const state = {
  options: null,
  dashboard: null,
  chartSelections: {
    dailyTotal: null,
    dailySuccess: null,
    providerHourly: null
  }
};

const CHART_COLORS = [
  '#0f766e',
  '#1d4ed8',
  '#dc6803',
  '#be123c',
  '#7c3aed',
  '#0891b2',
  '#65a30d',
  '#c2410c'
];

const elements = {
  from: document.querySelector('#from'),
  to: document.querySelector('#to'),
  app: document.querySelector('#app'),
  provider: document.querySelector('#provider'),
  username: document.querySelector('#username'),
  appVersion: document.querySelector('#app_version'),
  success: document.querySelector('#success'),
  apply: document.querySelector('#apply-filters'),
  healthText: document.querySelector('#health-text'),
  retentionText: document.querySelector('#retention-text'),
  timezoneText: document.querySelector('#timezone-text'),
  loadingOverlay: document.querySelector('#loading-overlay')
};

bootstrap().catch((error) => {
  console.error(error);
  setLoading(false);
  elements.healthText.textContent = '加载失败';
});

async function bootstrap() {
  setLoading(true);

  try {
    void loadHealth();
    await loadOptions();
    await loadDashboard();
  } finally {
    setLoading(false);
  }

  elements.apply.addEventListener('click', async () => {
    await refreshDashboard({ preserveSelections: true });
  });
}

async function refreshDashboard({ preserveSelections = false } = {}) {
  setLoading(true);

  try {
    await loadOptions({ preserveSelections });
    await loadDashboard();
  } finally {
    setLoading(false);
  }
}

async function loadHealth() {
  try {
    const data = await fetchJson('/healthz', { timeoutMs: 5000 });
    elements.healthText.textContent = data.ok ? '运行中' : '异常';
    elements.retentionText.textContent = `${data.retention_days} 天`;
    elements.timezoneText.textContent = data.time_zone;
  } catch (error) {
    console.error(error);
    elements.healthText.textContent = '检查失败';
  }
}

async function loadOptions({ preserveSelections = false } = {}) {
  const previousSelections = preserveSelections ? getSelections() : {};
  const params = new URLSearchParams();

  if (elements.from.value) {
    params.set('from', elements.from.value);
  }
  if (elements.to.value) {
    params.set('to', elements.to.value);
  }

  const data = await fetchJson(`/api/options?${params.toString()}`);
  state.options = data;

  if (!elements.from.value || !preserveSelections) {
    elements.from.value = data.range.from;
  }

  if (!elements.to.value || !preserveSelections) {
    elements.to.value = data.range.to;
  }

  fillSelect(elements.app, data.apps, previousSelections.app);
  fillSelect(elements.provider, data.providers, previousSelections.provider);
  fillSelect(elements.username, data.usernames, previousSelections.username);
  fillSelect(elements.appVersion, data.app_versions, previousSelections.app_version);
  elements.success.value = previousSelections.success || elements.success.value || 'all';
}

async function loadDashboard() {
  const params = new URLSearchParams();
  const selections = getSelections();

  for (const [key, value] of Object.entries(selections)) {
    if (value) {
      params.set(key, value);
    }
  }

  const data = await fetchJson(`/api/dashboard-data?${params.toString()}`);
  state.dashboard = data;

  renderDashboardViews();
}

function renderDashboardViews() {
  const data = state.dashboard;
  if (!data) {
    return;
  }

  renderSummary(data.summary);
  renderMultiLineChart('#daily-total-chart', data.daily_by_app, {
    selectionKey: 'dailyTotal',
    valueKey: 'total',
    metricLabel: '请求量',
    formatter: (value) => value.toLocaleString(),
    legendValue: (item) => Number(item.total || 0),
    tooltipLines: (point) => [
      `成功 ${Number(point.success_count || 0).toLocaleString()}`,
      `失败 ${Number(point.failure_count || 0).toLocaleString()}`,
      `成功率 ${Number(point.success_rate || 0).toFixed(2)}%`
    ]
  });
  renderMultiLineChart('#daily-success-chart', data.daily_by_app, {
    selectionKey: 'dailySuccess',
    valueKey: 'success_rate',
    metricLabel: '成功率',
    maxDomainValue: 100,
    tickValues: [0, 25, 50, 75, 100],
    formatter: (value) => `${value.toFixed(2)}%`,
    legendValue: (item) => getSeriesSuccessRate(item),
    tooltipLines: (point) => [
      `成功 ${Number(point.success_count || 0).toLocaleString()} / 总计 ${Number(point.total || 0).toLocaleString()}`,
      `失败 ${Number(point.failure_count || 0).toLocaleString()}`
    ]
  });
  renderMultiLineChart('#provider-hourly-chart', data.provider_hourly.slice(0, 6), {
    selectionKey: 'providerHourly',
    seriesKey: 'provider',
    seriesLabel: 'Provider',
    valueKey: 'total',
    metricLabel: '调用量',
    width: 920,
    xTickCount: 8,
    formatter: (value) => value.toLocaleString(),
    legendValue: (item) => Number(item.total || 0),
    tooltipLines: (point) => [
      `成功 ${Number(point.success_count || 0).toLocaleString()}`,
      `失败 ${Number(point.failure_count || 0).toLocaleString()}`,
      `成功率 ${Number(point.success_rate || 0).toFixed(2)}%`
    ]
  });
  renderStackedBars('#provider-chart', data.providers.slice(0, 8));
  renderStackedBars('#nat-provider-chart', data.nat_providers.slice(0, 8));
  renderBars('#app-chart', data.apps.slice(0, 8), {
    labelKey: 'app',
    valueKey: 'total',
    valueFormatter: (value) => `${value} 次`,
    color: '#1d4ed8'
  });
  renderCompositeBars('#user-chart', data.users.slice(0, 8), {
    primaryKey: 'app',
    secondaryKey: 'username',
    primaryLabel: 'App',
    secondaryLabel: '用户',
    valueKey: 'total',
    valueFormatter: (value) => `${value} 次`,
    color: '#7c3aed'
  });
  renderCompositeBars('#version-chart', data.versions.slice(0, 8), {
    primaryKey: 'app',
    secondaryKey: 'app_version',
    primaryLabel: 'App',
    secondaryLabel: '版本',
    valueKey: 'avg_duration_ms',
    valueFormatter: (value) => `${value.toFixed(2)} ms`,
    color: '#be123c'
  });
  renderTable('#provider-table', data.providers.slice(0, 12), 'provider');
  renderTable('#nat-provider-table', data.nat_providers.slice(0, 12), 'provider');
  renderTable('#app-table', data.apps.slice(0, 12), 'app');
}

function getSelections() {
  return {
    from: elements.from.value,
    to: elements.to.value,
    app: emptyAsUndefined(elements.app.value),
    provider: emptyAsUndefined(elements.provider.value),
    username: emptyAsUndefined(elements.username.value),
    app_version: emptyAsUndefined(elements.appVersion.value),
    success: elements.success.value || 'all'
  };
}

function emptyAsUndefined(value) {
  return value === '' ? undefined : value;
}

function fillSelect(selectElement, options, selectedValue) {
  const currentValue = selectedValue || selectElement.value;
  const entries = ['<option value="">全部</option>'].concat(
    options.map((option) => `<option value="${escapeHtml(option)}">${escapeHtml(option)}</option>`)
  );

  selectElement.innerHTML = entries.join('');
  if (currentValue && options.includes(currentValue)) {
    selectElement.value = currentValue;
  } else {
    selectElement.value = '';
  }
}

function renderSummary(summary) {
  document.querySelector('#summary-total').textContent = summary.total.toLocaleString();
  document.querySelector('#summary-success-rate').textContent = `${summary.success_rate.toFixed(2)}%`;
  document.querySelector('#summary-duration').textContent = `${summary.avg_duration_ms.toFixed(2)} ms`;
  document.querySelector('#summary-apps').textContent = summary.unique_apps.toLocaleString();
  document.querySelector('#summary-providers').textContent = summary.unique_providers.toLocaleString();
  document.querySelector('#summary-users').textContent = summary.unique_users.toLocaleString();
}

function setLoading(isLoading) {
  document.body.classList.toggle('is-loading', isLoading);
  elements.loadingOverlay.hidden = !isLoading;
  elements.loadingOverlay.style.display = isLoading ? 'grid' : 'none';
  elements.apply.disabled = isLoading;
  elements.apply.textContent = isLoading ? '加载中...' : '刷新看板';
}

function renderMultiLineChart(selector, series, config) {
  const target = document.querySelector(selector);
  if (!series.length || !series[0].points.length) {
    target.innerHTML = '<div class="empty-state">当前筛选条件下暂无数据</div>';
    return;
  }

  const seriesKey = config.seriesKey || 'app';
  const seriesLabel = config.seriesLabel || 'App';
  const selectedSeries = series.some((item) => item[seriesKey] === state.chartSelections[config.selectionKey])
    ? state.chartSelections[config.selectionKey]
    : null;
  state.chartSelections[config.selectionKey] = selectedSeries;
  const visibleSeries = selectedSeries
    ? series.filter((item) => item[seriesKey] === selectedSeries)
    : series;
  const effectiveSeries = visibleSeries.length > 0 ? visibleSeries : series;

  const width = config.width || 720;
  const height = config.height || 260;
  const padding = { top: 24, right: 20, bottom: 40, left: 112 };
  const pointsTemplate = series[0].points;
  const values = effectiveSeries.flatMap((item) => item.points.map((point) => Number(point[config.valueKey] || 0)));
  const { maxValue, tickValues } = resolveChartScale(values, config);
  const innerWidth = width - padding.left - padding.right;
  const innerHeight = height - padding.top - padding.bottom;
  const chartId = `chart-${config.selectionKey}`;
  const xTicks = buildTickIndexes(pointsTemplate.length, config.xTickCount || 5);
  const pointLookup = new Map();

  const xLabels = pointsTemplate
    .map((row, index) => ({ row, index }))
    .filter(({ index }) => xTicks.has(index))
    .map(({ row, index }) => {
      const x = padding.left + (index / Math.max(pointsTemplate.length - 1, 1)) * innerWidth;
      return `
        <line x1="${x}" y1="${padding.top}" x2="${x}" y2="${height - padding.bottom}" class="grid-line grid-line-vertical" />
        <text x="${x}" y="${height - 12}" text-anchor="middle">${row.date.slice(5)}</text>
      `;
    })
    .join('');

  const yTicks = tickValues
    .map((value) => {
      const y = padding.top + innerHeight - (value / maxValue) * innerHeight;
      return `
        <line x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}" class="grid-line" />
        <text x="10" y="${y + 4}" text-anchor="start">${config.formatter(value)}</text>
      `;
    })
    .join('');

  const defs = effectiveSeries.map((item, itemIndex) => {
    const originalIndex = series.findIndex((entry) => entry[seriesKey] === item[seriesKey]);
    const color = CHART_COLORS[(originalIndex >= 0 ? originalIndex : itemIndex) % CHART_COLORS.length];
    return `
      <linearGradient id="${chartId}-area-${itemIndex}" x1="0%" y1="0%" x2="0%" y2="100%">
        <stop offset="0%" stop-color="${color}" stop-opacity="0.24"></stop>
        <stop offset="100%" stop-color="${color}" stop-opacity="0"></stop>
      </linearGradient>
    `;
  }).join('');

  const polylines = effectiveSeries.map((item, itemIndex) => {
    const originalIndex = series.findIndex((entry) => entry[seriesKey] === item[seriesKey]);
    const color = CHART_COLORS[(originalIndex >= 0 ? originalIndex : itemIndex) % CHART_COLORS.length];
    const points = item.points.map((row, index) => {
      const x = padding.left + (index / Math.max(item.points.length - 1, 1)) * innerWidth;
      const y = padding.top + innerHeight - (Number(row[config.valueKey] || 0) / maxValue) * innerHeight;
      return `${x},${y}`;
    }).join(' ');
    const pointGroups = item.points.map((row, index) => {
      const x = padding.left + (index / Math.max(item.points.length - 1, 1)) * innerWidth;
      const y = padding.top + innerHeight - (Number(row[config.valueKey] || 0) / maxValue) * innerHeight;
      const pointId = `${config.selectionKey}-${itemIndex}-${index}`;
      pointLookup.set(pointId, {
        color,
        x,
        y,
        seriesValue: item[seriesKey],
        label: item.label,
        point: row,
        value: Number(row[config.valueKey] || 0)
      });
      return `
        <g class="chart-point-group" data-point-id="${pointId}">
          <circle cx="${x}" cy="${y}" r="10" class="chart-point-glow" fill="${color}"></circle>
          <circle cx="${x}" cy="${y}" r="4.5" class="chart-point-core" fill="${color}"></circle>
          <circle cx="${x}" cy="${y}" r="13" class="chart-point-hit" data-point-id="${pointId}" tabindex="0" aria-label="${escapeHtml(`${item.label} ${row.date} ${config.metricLabel} ${config.formatter(Number(row[config.valueKey] || 0))}`)}"></circle>
        </g>
      `;
    }).join('');
    const areaPoints = `${padding.left},${height - padding.bottom} ${points} ${padding.left + innerWidth},${height - padding.bottom}`;
    const area = effectiveSeries.length === 1
      ? `<polygon points="${areaPoints}" class="chart-area-fill" fill="url(#${chartId}-area-${itemIndex})"></polygon>`
      : '';

    return `
      <g class="chart-series" data-series-value="${escapeHtml(item[seriesKey])}">
        ${area}
        <polyline points="${points}" class="chart-line-shadow" stroke="${color}" filter="url(#${chartId}-glow)"></polyline>
        <polyline points="${points}" class="chart-line" stroke="${color}"></polyline>
        ${pointGroups}
      </g>
    `;
  }).join('');

  const legend = series.map((item, itemIndex) => {
    const color = CHART_COLORS[itemIndex % CHART_COLORS.length];
    const isActive = !selectedSeries || selectedSeries === item[seriesKey];
    const legendValue = typeof config.legendValue === 'function'
      ? config.legendValue(item)
      : Number(item.points[item.points.length - 1]?.[config.valueKey] || 0);
    return `
      <button
        type="button"
        class="legend-item legend-button${isActive ? ' is-active' : ' is-muted'}"
        data-selection-key="${config.selectionKey}"
        data-series-value="${escapeHtml(item[seriesKey])}"
        aria-pressed="${selectedSeries === item[seriesKey] ? 'true' : 'false'}"
      >
        <span class="legend-dot" style="background:${color}"></span>
        <span>${escapeHtml(item.label)}</span>
        <strong>${config.formatter(Number(legendValue || 0))}</strong>
      </button>
    `;
  }).join('');

  target.innerHTML = `
    <div class="chart-caption">${selectedSeries ? `已聚焦 ${escapeHtml(selectedSeries)}，悬浮数据点可查看具体数值，再次点击图例可恢复全部 ${seriesLabel}` : `按 ${seriesLabel} 分线展示，共 ${series.length} 条曲线；悬浮数据点查看具体数值，点击图例可单独查看某个 ${seriesLabel}`}</div>
    <div class="chart-legend">${legend}</div>
    <div class="chart-shell">
      <div class="chart-tooltip" hidden></div>
      <svg viewBox="0 0 ${width} ${height}" class="line-chart" role="img">
        <defs>
          <filter id="${chartId}-glow" x="-20%" y="-20%" width="140%" height="140%">
            <feGaussianBlur stdDeviation="6" result="coloredBlur"></feGaussianBlur>
            <feMerge>
              <feMergeNode in="coloredBlur"></feMergeNode>
              <feMergeNode in="SourceGraphic"></feMergeNode>
            </feMerge>
          </filter>
          ${defs}
        </defs>
        <rect x="${padding.left - 12}" y="${padding.top - 8}" width="${innerWidth + 24}" height="${innerHeight + 16}" rx="22" class="chart-panel"></rect>
        <line x1="${padding.left}" y1="${height - padding.bottom}" x2="${width - padding.right}" y2="${height - padding.bottom}" class="axis-line"></line>
        <line x1="${padding.left}" y1="${padding.top}" x2="${padding.left}" y2="${height - padding.bottom}" class="axis-line axis-line-y"></line>
        <line x1="${padding.left}" y1="${padding.top}" x2="${padding.left}" y2="${height - padding.bottom}" class="chart-guide-line" hidden></line>
        ${xLabels}
        ${yTicks}
        ${polylines}
      </svg>
    </div>
  `;

  target.querySelectorAll('.legend-button').forEach((button) => {
    button.addEventListener('click', () => {
      const { selectionKey, seriesValue } = button.dataset;
      state.chartSelections[selectionKey] = state.chartSelections[selectionKey] === seriesValue ? null : seriesValue;
      renderDashboardViews();
    });
  });

  setupLineChartTooltip(target, pointLookup, config);
}

function resolveChartScale(values, config) {
  if (Array.isArray(config.tickValues) && config.tickValues.length > 1) {
    return {
      maxValue: Number(config.maxDomainValue || config.tickValues[config.tickValues.length - 1] || 1),
      tickValues: config.tickValues
    };
  }

  const maxInput = Math.max(...values, 1);
  const maxValue = Number(config.maxDomainValue || getNiceMaxValue(maxInput));
  return {
    maxValue,
    tickValues: buildTickValues(maxValue)
  };
}

function getNiceMaxValue(value) {
  const rawStep = getNiceStep(value / 4);
  return Math.max(rawStep, Math.ceil(value / rawStep) * rawStep, 1);
}

function getNiceStep(value) {
  if (!Number.isFinite(value) || value <= 1) {
    return 1;
  }

  const magnitude = 10 ** Math.floor(Math.log10(value));
  const normalized = value / magnitude;

  if (normalized <= 1) {
    return magnitude;
  }
  if (normalized <= 2) {
    return 2 * magnitude;
  }
  if (normalized <= 2.5) {
    return 2.5 * magnitude;
  }
  if (normalized <= 5) {
    return 5 * magnitude;
  }
  return 10 * magnitude;
}

function buildTickValues(maxValue) {
  if (maxValue <= 4) {
    return Array.from({ length: maxValue + 1 }, (_, index) => index);
  }

  const step = getNiceStep(maxValue / 4);
  const ticks = [];

  for (let value = 0; value <= maxValue + step / 2; value += step) {
    ticks.push(Number(value.toFixed(4)));
  }

  if (ticks[ticks.length - 1] !== maxValue) {
    ticks[ticks.length - 1] = maxValue;
  }

  return ticks;
}

function buildTickIndexes(length, desiredCount) {
  if (length <= 1) {
    return new Set([0]);
  }

  const total = Math.min(length, Math.max(desiredCount, 2));
  const indexes = new Set();

  for (let step = 0; step < total; step += 1) {
    const index = Math.round((step / (total - 1)) * (length - 1));
    indexes.add(index);
  }

  return indexes;
}

function setupLineChartTooltip(target, pointLookup, config) {
  const tooltip = target.querySelector('.chart-tooltip');
  const shell = target.querySelector('.chart-shell');
  const svg = target.querySelector('.line-chart');
  const guideLine = target.querySelector('.chart-guide-line');
  let activePointId = null;

  if (!tooltip || !shell || !svg || !guideLine) {
    return;
  }

  const updateSeriesState = (pointId) => {
    const activePoint = pointId ? pointLookup.get(pointId) : null;

    target.querySelectorAll('.chart-series').forEach((seriesGroup) => {
      const isActive = !activePoint || seriesGroup.dataset.seriesValue === activePoint.seriesValue;
      seriesGroup.classList.toggle('is-highlighted', Boolean(activePoint) && isActive);
      seriesGroup.classList.toggle('is-dimmed', Boolean(activePoint) && !isActive);
    });

    target.querySelectorAll('.chart-point-group').forEach((pointGroup) => {
      pointGroup.classList.toggle('is-active', pointGroup.dataset.pointId === pointId);
    });
  };

  const positionTooltip = (point) => {
    const scaleX = svg.clientWidth / svg.viewBox.baseVal.width;
    const scaleY = svg.clientHeight / svg.viewBox.baseVal.height;
    const pointX = point.x * scaleX;
    const pointY = point.y * scaleY;
    const tooltipWidth = tooltip.offsetWidth;
    const tooltipHeight = tooltip.offsetHeight;
    let left = pointX + 18;
    let top = pointY - tooltipHeight - 14;

    if (left + tooltipWidth > shell.clientWidth - 8) {
      left = pointX - tooltipWidth - 18;
    }
    if (left < 8) {
      left = 8;
    }
    if (top < 8) {
      top = pointY + 18;
    }
    if (top + tooltipHeight > shell.clientHeight - 8) {
      top = shell.clientHeight - tooltipHeight - 8;
    }

    tooltip.style.left = `${left}px`;
    tooltip.style.top = `${top}px`;
  };

  const showTooltip = (pointId) => {
    if (!pointLookup.has(pointId)) {
      return;
    }

    activePointId = pointId;
    const { label, point, value, color, x } = pointLookup.get(pointId);
    const extraLines = typeof config.tooltipLines === 'function' ? config.tooltipLines(point) : [];
    const tooltipRows = extraLines.map((line) => `<div class="chart-tooltip-row">${escapeHtml(line)}</div>`).join('');

    tooltip.innerHTML = `
      <div class="chart-tooltip-title">${escapeHtml(label)}</div>
      <div class="chart-tooltip-date">${escapeHtml(point.date)}</div>
      <div class="chart-tooltip-value">
        <span class="chart-tooltip-swatch" style="background:${color}"></span>
        <span>${escapeHtml(config.metricLabel)}</span>
        <strong>${escapeHtml(config.formatter(value))}</strong>
      </div>
      ${tooltipRows}
    `;
    tooltip.hidden = false;
    guideLine.setAttribute('x1', x);
    guideLine.setAttribute('x2', x);
    guideLine.hidden = false;
    positionTooltip(pointLookup.get(pointId));
    updateSeriesState(pointId);
  };

  const hideTooltip = () => {
    activePointId = null;
    tooltip.hidden = true;
    guideLine.hidden = true;
    updateSeriesState(null);
  };

  target.querySelectorAll('.chart-point-hit').forEach((pointNode) => {
    pointNode.addEventListener('mouseenter', () => {
      showTooltip(pointNode.dataset.pointId);
    });
    pointNode.addEventListener('mousemove', () => {
      if (activePointId) {
        positionTooltip(pointLookup.get(activePointId));
      }
    });
    pointNode.addEventListener('mouseleave', hideTooltip);
    pointNode.addEventListener('focus', () => {
      showTooltip(pointNode.dataset.pointId);
    });
    pointNode.addEventListener('blur', hideTooltip);
    pointNode.addEventListener('click', () => {
      showTooltip(pointNode.dataset.pointId);
    });
  });

  shell.addEventListener('mouseleave', hideTooltip);
}

function getSeriesSuccessRate(item) {
  const totals = item.points.reduce((accumulator, point) => {
    accumulator.total += Number(point.total || 0);
    accumulator.successCount += Number(point.success_count || 0);
    return accumulator;
  }, { total: 0, successCount: 0 });

  if (totals.total === 0) {
    return 0;
  }

  return Number(((totals.successCount / totals.total) * 100).toFixed(2));
}

function renderStackedBars(selector, rows) {
  const target = document.querySelector(selector);
  if (!rows.length) {
    target.innerHTML = '<div class="empty-state">当前筛选条件下暂无数据</div>';
    return;
  }

  target.innerHTML = rows.map((row) => {
    const successWidth = row.total === 0 ? 0 : (row.success_count / row.total) * 100;
    const failureWidth = 100 - successWidth;
    return `
      <div class="bar-row">
        <div class="bar-head">
          <span>${escapeHtml(row.provider)}</span>
          <strong>${row.success_rate.toFixed(2)}%</strong>
        </div>
        <div class="stacked-track">
          <span class="stack success" style="width: ${successWidth}%"></span>
          <span class="stack failure" style="width: ${failureWidth}%"></span>
        </div>
        <div class="bar-foot">
          <span>成功 ${row.success_count}</span>
          <span>失败 ${row.failure_count}</span>
          <span>总计 ${row.total}</span>
        </div>
      </div>
    `;
  }).join('');
}

function renderBars(selector, rows, config) {
  const target = document.querySelector(selector);
  if (!rows.length) {
    target.innerHTML = '<div class="empty-state">当前筛选条件下暂无数据</div>';
    return;
  }

  const maxValue = Math.max(...rows.map((row) => Number(row[config.valueKey] || 0)), 1);
  target.innerHTML = rows.map((row) => {
    const value = Number(row[config.valueKey] || 0);
    const width = (value / maxValue) * 100;
    return `
      <div class="bar-row">
        <div class="bar-head">
          <span>${escapeHtml(row[config.labelKey])}</span>
          <strong>${config.valueFormatter(value)}</strong>
        </div>
        <div class="single-track">
          <span class="single-fill" style="width: ${width}%; background: ${config.color};"></span>
        </div>
      </div>
    `;
  }).join('');
}

function renderCompositeBars(selector, rows, config) {
  const target = document.querySelector(selector);
  if (!rows.length) {
    target.innerHTML = '<div class="empty-state">当前筛选条件下暂无数据</div>';
    return;
  }

  const maxValue = Math.max(...rows.map((row) => Number(row[config.valueKey] || 0)), 1);
  target.innerHTML = rows.map((row) => {
    const value = Number(row[config.valueKey] || 0);
    const width = (value / maxValue) * 100;
    return `
      <div class="bar-row">
        <div class="bar-head">
          <div class="bar-label">
            <strong class="bar-title">${escapeHtml(config.primaryLabel)}: ${escapeHtml(row[config.primaryKey])}</strong>
            <span class="bar-subtitle">${escapeHtml(config.secondaryLabel)}: ${escapeHtml(row[config.secondaryKey])}</span>
          </div>
          <strong>${config.valueFormatter(value)}</strong>
        </div>
        <div class="single-track">
          <span class="single-fill" style="width: ${width}%; background: ${config.color};"></span>
        </div>
      </div>
    `;
  }).join('');
}

function renderTable(selector, rows, keyName) {
  const target = document.querySelector(selector);
  if (!rows.length) {
    target.innerHTML = '<div class="empty-state">当前筛选条件下暂无数据</div>';
    return;
  }

  const headLabel = keyName === 'provider' ? 'Provider' : 'App';
  const body = rows.map((row) => `
    <tr>
      <td>${escapeHtml(row[keyName])}</td>
      <td>${row.total}</td>
      <td>${row.success_rate.toFixed(2)}%</td>
      <td>${row.avg_duration_ms.toFixed(2)} ms</td>
    </tr>
  `).join('');

  target.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>${headLabel}</th>
          <th>请求量</th>
          <th>成功率</th>
          <th>平均耗时</th>
        </tr>
      </thead>
      <tbody>${body}</tbody>
    </table>
  `;
}

async function fetchJson(url, { timeoutMs = 15000 } = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      cache: 'no-store',
      signal: controller.signal
    });

    if (!response.ok) {
      throw new Error(`Request failed: ${response.status} ${response.statusText}`);
    }

    return await response.json();
  } finally {
    clearTimeout(timer);
  }
}

function escapeHtml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

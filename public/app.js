const state = {
  options: null,
  dashboard: null,
  chartSelections: {
    dailyTotal: null,
    dailySuccess: null
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
    formatter: (value) => value.toLocaleString()
  });
  renderMultiLineChart('#daily-success-chart', data.daily_by_app, {
    selectionKey: 'dailySuccess',
    valueKey: 'success_rate',
    formatter: (value) => `${value.toFixed(2)}%`
  });
  renderStackedBars('#provider-chart', data.providers.slice(0, 8));
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

  const selectedApp = series.some((item) => item.app === state.chartSelections[config.selectionKey])
    ? state.chartSelections[config.selectionKey]
    : null;
  state.chartSelections[config.selectionKey] = selectedApp;
  const visibleSeries = selectedApp
    ? series.filter((item) => item.app === selectedApp)
    : series;
  const effectiveSeries = visibleSeries.length > 0 ? visibleSeries : series;

  const width = 720;
  const height = 260;
  const padding = { top: 24, right: 20, bottom: 40, left: 112 };
  const pointsTemplate = series[0].points;
  const values = effectiveSeries.flatMap((item) => item.points.map((point) => Number(point[config.valueKey] || 0)));
  const maxValue = Math.max(...values, 1);
  const innerWidth = width - padding.left - padding.right;
  const innerHeight = height - padding.top - padding.bottom;

  const tickIndexes = new Set([
    0,
    Math.floor((pointsTemplate.length - 1) / 3),
    Math.floor(((pointsTemplate.length - 1) * 2) / 3),
    pointsTemplate.length - 1
  ]);

  const xLabels = pointsTemplate
    .map((row, index) => ({ row, index }))
    .filter(({ index }) => tickIndexes.has(index))
    .map(({ row, index }) => {
      const x = padding.left + (index / Math.max(pointsTemplate.length - 1, 1)) * innerWidth;
      return `<text x="${x}" y="${height - 12}" text-anchor="middle">${row.date.slice(5)}</text>`;
    })
    .join('');

  const yTicks = [0, maxValue / 2, maxValue]
    .map((value) => {
      const y = padding.top + innerHeight - (value / maxValue) * innerHeight;
      return `
        <line x1="${padding.left}" y1="${y}" x2="${width - padding.right}" y2="${y}" class="grid-line" />
        <text x="10" y="${y + 4}" text-anchor="start">${config.formatter(value)}</text>
      `;
    })
    .join('');

  const polylines = effectiveSeries.map((item, itemIndex) => {
    const originalIndex = series.findIndex((entry) => entry.app === item.app);
    const color = CHART_COLORS[(originalIndex >= 0 ? originalIndex : itemIndex) % CHART_COLORS.length];
    const points = item.points.map((row, index) => {
      const x = padding.left + (index / Math.max(item.points.length - 1, 1)) * innerWidth;
      const y = padding.top + innerHeight - (Number(row[config.valueKey] || 0) / maxValue) * innerHeight;
      return `${x},${y}`;
    }).join(' ');

    return `<polyline points="${points}" fill="none" stroke="${color}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"></polyline>`;
  }).join('');

  const legend = series.map((item, itemIndex) => {
    const color = CHART_COLORS[itemIndex % CHART_COLORS.length];
    const lastPoint = item.points[item.points.length - 1];
    const isActive = !selectedApp || selectedApp === item.app;
    return `
      <button
        type="button"
        class="legend-item legend-button${isActive ? ' is-active' : ' is-muted'}"
        data-selection-key="${config.selectionKey}"
        data-app="${escapeHtml(item.app)}"
        aria-pressed="${selectedApp === item.app ? 'true' : 'false'}"
      >
        <span class="legend-dot" style="background:${color}"></span>
        <span>${escapeHtml(item.label)}</span>
        <strong>${config.formatter(Number(lastPoint[config.valueKey] || 0))}</strong>
      </button>
    `;
  }).join('');

  target.innerHTML = `
    <div class="chart-caption">${selectedApp ? `已聚焦 ${escapeHtml(selectedApp)}，再次点击可恢复全部 App` : `按 App 分线展示，共 ${series.length} 条曲线，点击图例可单独查看某个 App`}</div>
    <div class="chart-legend">${legend}</div>
    <svg viewBox="0 0 ${width} ${height}" class="line-chart" role="img">
      ${yTicks}
      ${polylines}
      ${xLabels}
    </svg>
  `;

  target.querySelectorAll('.legend-button').forEach((button) => {
    button.addEventListener('click', () => {
      const { selectionKey, app } = button.dataset;
      state.chartSelections[selectionKey] = state.chartSelections[selectionKey] === app ? null : app;
      renderDashboardViews();
    });
  });
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

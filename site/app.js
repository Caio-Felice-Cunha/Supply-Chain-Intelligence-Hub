const number = new Intl.NumberFormat('en-CA');
const money = new Intl.NumberFormat('en-CA', { style: 'currency', currency: 'CAD', maximumFractionDigits: 0 });
const percent = new Intl.NumberFormat('en-CA', { maximumFractionDigits: 1 });

const byId = (id) => document.getElementById(id);
const option = (value, label) => Object.assign(document.createElement('option'), { value, textContent: label });

function filteredRows(data) {
  const start = byId('startPeriod').value;
  const end = byId('endPeriod').value;
  const warehouse = byId('warehouse').value;
  const category = byId('category').value;
  const supplier = byId('supplier').value;
  return data.cube.filter((row) => (!start || row.period >= start) && (!end || row.period <= end)
    && (!warehouse || String(row.warehouse_id) === warehouse) && (!category || row.category === category)
    && (!supplier || String(row.supplier_id) === supplier));
}

function summarize(rows) {
  const sum = (field) => rows.reduce((total, row) => total + Number(row[field] || 0), 0);
  const completed = sum('completed_orders');
  const latest = rows.reduce((value, row) => row.period > value ? row.period : value, '');
  const pointInTime = rows.filter((row) => row.period === latest);
  const risk = new Set();
  pointInTime.forEach((row) => String(row.risk_sku_ids || '').split(',').filter(Boolean).forEach((id) => risk.add(id)));
  return { revenue: sum('revenue'), units: sum('units'), otd: completed ? sum('on_time_orders') * 100 / completed : 0, risk: risk.size };
}

function monthly(rows) {
  const result = new Map();
  rows.forEach((row) => {
    const current = result.get(row.period) || { period: row.period, revenue: 0, units: 0 };
    current.revenue += Number(row.revenue || 0); current.units += Number(row.units || 0); result.set(row.period, current);
  });
  return [...result.values()].sort((a, b) => a.period.localeCompare(b.period));
}

function linePath(values, width, height, maximum) {
  if (!values.length) return '';
  return values.map((value, index) => `${index ? 'L' : 'M'} ${40 + index * ((width - 60) / Math.max(1, values.length - 1))} ${height - 30 - (value / Math.max(1, maximum)) * (height - 55)}`).join(' ');
}

function drawSalesChart(values) {
  const width = 760; const height = 260;
  const maxRevenue = Math.max(...values.map((item) => item.revenue), 1);
  const maxUnits = Math.max(...values.map((item) => item.units), 1);
  const grid = [0, .25, .5, .75, 1].map((ratio) => `<line class="gridline" x1="40" x2="740" y1="${height - 30 - ratio * (height - 55)}" y2="${height - 30 - ratio * (height - 55)}"/>`).join('');
  const labels = values.map((item, index) => index % Math.max(1, Math.ceil(values.length / 6)) === 0 ? `<text class="axis-label" x="${40 + index * ((width - 60) / Math.max(1, values.length - 1))}" y="252" text-anchor="middle">${item.period}</text>` : '').join('');
  byId('salesChart').innerHTML = `<svg viewBox="0 0 ${width} ${height}" aria-hidden="true">${grid}<path class="revenue-line" d="${linePath(values.map((item) => item.revenue), width, height, maxRevenue)}"/><path class="unit-line" d="${linePath(values.map((item) => item.units), width, height, maxUnits)}"/>${labels}</svg>`;
  byId('salesChart').setAttribute('aria-label', `Monthly sales from ${values[0]?.period || 'no period'} to ${values.at(-1)?.period || 'no period'}`);
  byId('monthlyRows').replaceChildren(...values.map((item) => { const row = document.createElement('tr'); [item.period, money.format(item.revenue), number.format(item.units)].forEach((value) => { const cell = document.createElement('td'); cell.textContent = value; row.append(cell); }); return row; }));
}

function renderInventory(rows) {
  const latest = rows.reduce((value, row) => row.period > value ? row.period : value, '');
  rows = rows.filter((row) => row.period === latest);
  const statuses = ['critical', 'low', 'optimal', 'excess'];
  const totals = Object.fromEntries(statuses.map((status) => [status, rows.reduce((sum, row) => sum + Number(row[`${status}_positions`] || 0), 0)]));
  const maximum = Math.max(...Object.values(totals), 1);
  byId('inventoryBars').replaceChildren(...statuses.map((status) => { const wrapper = document.createElement('div'); wrapper.className = 'health-row'; wrapper.innerHTML = `<span>${status}</span><span class="health-track"><i class="health-fill ${status}" style="width:${totals[status] * 100 / maximum}%"></i></span><strong>${number.format(totals[status])}</strong>`; return wrapper; }));
}

function renderSuppliers(rows, dimensions) {
  const grouped = new Map();
  rows.forEach((row) => { const item = grouped.get(row.supplier_id) || { orders: 0, completed: 0, onTime: 0, late: 0 }; item.orders += Number(row.purchase_orders || 0); item.completed += Number(row.completed_orders || 0); item.onTime += Number(row.on_time_orders || 0); item.late += Number(row.late_days || 0); grouped.set(row.supplier_id, item); });
  const names = new Map(dimensions.suppliers.map((item) => [item.supplier_id, item.supplier_name]));
  const values = [...grouped].map(([id, item]) => ({ id, name: names.get(id), ...item, rate: item.completed ? item.onTime * 100 / item.completed : 0 })).sort((a, b) => b.orders - a.orders).slice(0, 8);
  byId('supplierMeta').textContent = `${grouped.size} suppliers in selection`;
  byId('supplierRows').replaceChildren(...values.map((item) => { const row = document.createElement('tr'); row.innerHTML = `<td>${item.name}</td><td>${number.format(item.orders)}</td><td><span class="rate"><span class="mini-track"><i style="width:${item.rate}%"></i></span>${percent.format(item.rate)}%</span></td><td>${number.format(item.late)}</td>`; return row; }));
}

function renderDashboard(data) {
  const rows = filteredRows(data); const summary = summarize(rows); const series = monthly(rows);
  byId('kpiRevenue').textContent = money.format(summary.revenue); byId('kpiUnits').textContent = number.format(summary.units);
  byId('kpiOtd').textContent = `${percent.format(summary.otd)}%`; byId('kpiRisk').textContent = number.format(summary.risk);
  drawSalesChart(series); renderInventory(rows); renderSuppliers(rows, data.dimensions);
}

function renderEvidence(data, samples, manifest) {
  byId('runBadge').textContent = `${number.format(data.meta.source_rows)} rows · seed ${data.meta.seed} · ${data.quality.failed ? 'CHECK' : 'ALL CONTRACTS PASS'}`;
  byId('qualityPassed').textContent = data.quality.passed; byId('qualityFailed').textContent = data.quality.failed;
  const labels = { demand_spikes: 'Demand spikes', extreme_delays: 'Extreme delays', price_jumps: 'Price jumps', controlled_stock_risks: 'Stock-risk snapshots' };
  byId('anomalyGrid').replaceChildren(...Object.entries(data.anomalies).map(([key, value]) => { const card = document.createElement('div'); card.innerHTML = `<strong>${number.format(value)}</strong><span>${labels[key]}</span>`; return card; }));
  byId('lineage').replaceChildren(...data.lineage.map((stage, index) => { const item = document.createElement('li'); item.innerHTML = `${String(index + 1).padStart(2, '0')} ${stage.stage}<small>${stage.state}</small>`; return item; }));
  byId('lineageRows').replaceChildren(...data.lineage.map((stage) => { const row = document.createElement('tr'); [stage.stage, stage.state, number.format(stage.rows_in), number.format(stage.rows_out), `${number.format(stage.duration_ms)} ms`].forEach((value) => { const cell = document.createElement('td'); cell.textContent = value; row.append(cell); }); return row; }));
  const sampleSelect = byId('sampleTable'); Object.keys(samples).forEach((name) => sampleSelect.append(option(name, `${name} · ${number.format(manifest.source_counts[name])} rows`)));
  const showSample = () => { const rows = samples[sampleSelect.value] || []; if (!rows.length) return; const columns = Object.keys(rows[0]); const table = document.createElement('table'); const head = document.createElement('thead'); head.innerHTML = `<tr>${columns.map((column) => `<th>${column}</th>`).join('')}</tr>`; const body = document.createElement('tbody'); rows.forEach((record) => { const row = document.createElement('tr'); columns.forEach((column) => { const cell = document.createElement('td'); cell.textContent = record[column] ?? '—'; row.append(cell); }); body.append(row); }); table.append(head, body); byId('sampleGrid').replaceChildren(table); };
  sampleSelect.addEventListener('change', showSample); showSample();
}

async function boot() {
  try {
    const [data, samples, manifest] = await Promise.all(['data/dashboard.json', 'data/samples.json', 'data/manifest.json'].map(async (url) => { const response = await fetch(url); if (!response.ok) throw new Error(`${url}: ${response.status}`); return response.json(); }));
    data.dimensions.periods.forEach((period) => { byId('startPeriod').append(option(period, period)); byId('endPeriod').append(option(period, period)); });
    byId('startPeriod').value = data.dimensions.periods[0]; byId('endPeriod').value = data.dimensions.periods.at(-1);
    data.dimensions.warehouses.forEach((item) => byId('warehouse').append(option(item.warehouse_id, `${item.warehouse_name} · ${item.location}`)));
    data.dimensions.categories.forEach((item) => byId('category').append(option(item, item)));
    data.dimensions.suppliers.forEach((item) => byId('supplier').append(option(item.supplier_id, item.supplier_name)));
    const filters = byId('filters'); filters.addEventListener('change', () => renderDashboard(data)); filters.addEventListener('reset', () => setTimeout(() => { byId('startPeriod').value = data.dimensions.periods[0]; byId('endPeriod').value = data.dimensions.periods.at(-1); renderDashboard(data); }));
    renderDashboard(data); renderEvidence(data, samples, manifest);
  } catch (error) { byId('errorState').hidden = false; byId('runBadge').textContent = 'Artifact unavailable'; console.error(error); }
}

boot();

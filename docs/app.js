const $ = id => document.getElementById(id), el = (tag, text = '', parent) => { const node = document.createElement(tag); node.textContent = text; parent.append(node); return node; }, clear = node => node.replaceChildren();
const controls = $('controls'), workspace = $('workspace');
function panel(title, id, wide = false) { const section = el('section', '', workspace); section.className = `panel${wide ? ' wide' : ''}`; const h = el('h2', title, section); h.id = `${id}-title`; section.setAttribute('aria-labelledby', h.id); return section; }
function control(tag, id, label, parent = controls) { const wrap = el('label', '', parent); el('span', label, wrap); const input = el(tag, '', wrap); input.id = id; return input; }
let data, runToken = 0, busy = false;
function formatUsd(cents) { return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(cents / 100); }
function makeUI() { controls.className = 'controls'; const borough = control('select', 'borough', 'Borough'); new Option('All boroughs', 'all'); borough.add(new Option('All boroughs', 'all')); const fault = control('select', 'fault', 'Simulated fault'); [['No fault', 'none'], ['Fault once, then recover', 'once'], ['Fault every attempt', 'always']].forEach(([label, value]) => fault.add(new Option(label, value))); const run = el('button', 'Replay five stages', controls); run.type = 'button'; run.id = 'run'; const reset = el('button', 'Reset simulation', controls); reset.type = 'button'; reset.id = 'reset'; reset.className = 'secondary'; const status = panel('Simulated browser replay', 'run', true); el('p', 'Ready for a browser-only replay.', status).id = 'run-status'; const list = el('ol', '', status); list.className = 'list'; list.id = 'stages'; const quality = panel('Source quality · actual fixture', 'quality'); el('div', '', quality).id = 'quality'; const lineage = panel('Lineage & pinned source', 'lineage'); el('div', '', lineage).id = 'lineage'; const tablePanel = panel('Previously published daily aggregate · fixture', 'daily', true); el('p', 'This actual aggregate never changes when a simulated job fails or retries.', tablePanel); const scroll = el('div', '', tablePanel); scroll.className = 'scroll'; const table = el('table', '', scroll), caption = el('caption', 'NYC TLC daily borough aggregates', table); caption.className = 'visually-hidden'; const head = el('thead', '', table), tr = el('tr', '', head); [['Day', ''], ['Borough', ''], ['Trips', 'num'], ['Net · USD', 'num'], ['Warning rows', 'num']].forEach(([text, cls]) => { const th = el('th', text, tr); th.scope = 'col'; if (cls)
    th.className = cls; }); el('tbody', '', table).id = 'daily'; borough.addEventListener('change', renderDaily); run.addEventListener('click', runReplay); reset.addEventListener('click', resetReplay); }
const count = value => Number.isFinite(value) ? value.toLocaleString() : 'unavailable';
function showQuality() { const root = $('quality'); clear(root); [['Accepted rows', count(data.summary.accepted)], ['Rejected rows', count(data.summary.rejected)], ['Warning rows', count(data.summary.warning_rows)], ['Source rows', count(data.source_rows)], ['Net recorded total · USD', formatUsd(data.summary.net_cents)]].forEach(([label, value]) => { const p = el('p', '', root); el('strong', `${value} `, p); el('span', label, p); }); }
function showLineage() { const root = $('lineage'); clear(root); const link = el('a', data.source_url, root); link.href = data.source_url; link.target = '_blank'; link.rel = 'noreferrer'; const hash = data.source_sha256 || data.sha256 || null; el('p', `Fixture rows ${data.source_rows.toLocaleString()} · daily aggregate rows ${data.daily.length} · source hash ${hash || 'not supplied in this fixture'}`, root); el('p', data.data_kind, root); }
function renderDaily() { if (!data)
    return; const tbody = $('daily'); clear(tbody); const rows = data.daily.filter(row => $('borough').value === 'all' || row.borough === $('borough').value).slice().sort((a, b) => a.day.localeCompare(b.day) || a.borough.localeCompare(b.borough)); rows.forEach(row => { const tr = el('tr', '', tbody); [row.day, row.borough, row.trips.toLocaleString(), formatUsd(row.net_cents), row.warning_rows.toLocaleString()].forEach((value, i) => { const td = el('td', String(value), tr); if (i > 1)
    td.className = 'num'; }); }); if (!rows.length) {
    const tr = el('tr', '', tbody), td = el('td', 'No aggregate rows for this borough.', tr);
    td.colSpan = 5;
} }
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
async function runReplay() { if (busy || !data)
    return; busy = true; const token = ++runToken, button = $('run'); button.disabled = true; $('stages').replaceChildren(); const status = $('run-status'), fault = $('fault').value; let recovered = false; for (let i = 0; i < data.stages.length; i++) {
    if (token !== runToken)
        return;
    const li = el('li', `${data.stages[i]} · running`, $('stages'));
    status.textContent = `Stage ${i + 1} of ${data.stages.length}: ${data.stages[i]}`;
    await pause(280);
    if (token !== runToken)
        return;
    if (fault !== 'none' && i === 2 && !recovered) {
        let attempt = 0;
        while (attempt < 3) {
            attempt++;
            li.textContent = `${data.stages[i]} · attempt ${attempt}: simulated fault`;
            status.textContent = `Stage ${i + 1}: simulated fault, retry ${attempt} / 3.`;
            await pause(240);
            if (token !== runToken)
                return;
            if (fault === 'once') {
                recovered = true;
                li.textContent = `${data.stages[i]} · retry ${attempt}: recovered on attempt ${attempt + 1}`;
                break;
            }
        }
        if (fault === 'always') {
            li.textContent = `${data.stages[i]} · failed after 3 attempts`;
            status.textContent = 'Replay failed after 3 attempts. The published aggregate above is unchanged.';
            button.disabled = false;
            busy = false;
            return;
        }
    }
    else
        li.textContent = `${data.stages[i]} · complete`;
    await pause(120);
} if (token === runToken) {
    status.textContent = recovered ? 'Replay completed after one simulated retry. No source was ingested.' : 'Replay completed; every stage was simulated in this browser. No source was ingested.';
    button.disabled = false;
    busy = false;
} }
function resetReplay() { runToken++; busy = false; $('run').disabled = false; $('fault').value = 'none'; $('stages').replaceChildren(); $('run-status').textContent = 'Simulation cleared. Actual pinned aggregate remains unchanged.'; }
makeUI();
fetch('./demo-data.json').then(response => { if (!response.ok)
    throw Error('Fixture unavailable'); return response.json(); }).then(json => { if (!json.summary || !Array.isArray(json.daily) || !Array.isArray(json.stages) || json.stages.length !== 5 || !json.daily.every(r => typeof r.day === 'string' && typeof r.borough === 'string' && Number.isFinite(r.trips) && Number.isFinite(r.net_cents) && Number.isFinite(r.warning_rows)))
    throw Error('Unexpected aggregate fixture shape'); data = json; [...new Set(data.daily.map(r => r.borough))].sort().forEach(name => $('borough').add(new Option(name, name))); showQuality(); showLineage(); renderDaily(); $('status').textContent = `Actual aggregate fixture loaded · ${data.daily.length} daily borough rows · ${data.source_rows.toLocaleString()} source records. Browser replay is simulated.`; $('status').className = 'status'; }).catch(error => { $('status').textContent = `Cannot load aggregate fixture: ${error.message}. Reload to try again.`; $('status').className = 'status error'; });
function labelScrollableContent() {
    document.querySelectorAll('.scroll, pre').forEach(node => {
        node.tabIndex = 0;
        if (!node.hasAttribute('role'))
            node.setAttribute('role', 'region');
        if (!node.hasAttribute('aria-label'))
            node.setAttribute('aria-label', `${node.closest('section')?.querySelector('h2')?.textContent || 'Recorded'} scrollable content`);
    });
}
new MutationObserver(labelScrollableContent).observe(document.getElementById('main'), { childList: true, subtree: true });
labelScrollableContent();

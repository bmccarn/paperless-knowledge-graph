import assert from 'node:assert/strict';
import { test } from 'node:test';
import { GRAPH_VIEWS, viewForKey } from '../.test-build/graph-url-state.js';

test('Table is listed and reachable with key 4 and t', () => {
  assert.equal(GRAPH_VIEWS.find(view => view.id === 'table').listed, true);
  assert.equal(viewForKey('4'), 'table');
  assert.equal(viewForKey('t'), 'table');
});

const { tableRows, sortTableRows, TABLE_PAGE_SIZE, TABLE_COLUMNS } = await import('../.test-build/graph-table.js');
const { neighborhoodLayout } = await import('../.test-build/graph-layout.js');
const summary = {
  focus: { id: 'root', label: 'Person', name: 'Synthetic root', is_hub: true }, owner_duplicates: [],
  documents: { 1: { date: '2025-01-01' }, 2: { date: '2026-01-01' } },
  links: Array.from({ length: 163 }, (_, i) => ({
    key: `root~EMPLOYED_BY~org-${String(i).padStart(3, '0')}`, type: 'EMPLOYED_BY', family: 'parties', direction: i % 2 ? 'in' : 'out',
    support_state: i % 3 ? 'supported' : 'inferred', stated_count: 1, inferred_count: 0,
    records: [{ doc_id: 1, confidence: i % 3 ? 0.9 : null }, { doc_id: 2, confidence: 0.7 }],
    neighbor: { id: `org-${i}`, label: 'Organization', name: `Organization ${i}` },
  })),
};

test('all semantic links page once and only Focus links get On canvas, including after expansion', () => {
  for (const cap of [2, 5, 6, 152]) {
    const layout = neighborhoodLayout(summary, cap);
    const rows = tableRows(summary, layout);
    assert.equal(rows.length, summary.links.length);
    assert.deepEqual(rows.filter(row => row.onCanvas).map(row => row.link.key).sort(), layout.links.filter(link => link.link).map(link => link.key).sort());
    const pages = Array.from({ length: Math.ceil(rows.length / TABLE_PAGE_SIZE) }, (_, index) => rows.slice(index * TABLE_PAGE_SIZE, (index + 1) * TABLE_PAGE_SIZE));
    assert.deepEqual(pages.map(page => page.length), [50, 50, 50, 13]);
    assert.equal(new Set(pages.flat().map(row => row.link.key)).size, rows.length);
  }
});

test('table uses distinct documents, minimum confidence and latest date; any absent score means No score', () => {
  const custom = structuredClone(summary);
  custom.links[1].records.push({ doc_id: 2, confidence: 0.5 });
  const rows = tableRows(custom, neighborhoodLayout(custom, 2));
  assert.equal(rows[1].documents, 2);
  assert.equal(rows[1].confidence, 0.5);
  assert.equal(rows[1].latest, '2026-01-01');
  assert.equal(rows[0].confidence, null);
  custom.links[0].records = [{ doc_id: 3, confidence: null }];
  assert.equal(tableRows(custom, neighborhoodLayout(custom, 2))[0].latest, null);
});

test('each column sorts without mutating rows, ties are stable, and missing values stay last', () => {
  const rows = tableRows(summary, neighborhoodLayout(summary, 2));
  const original = rows.map(row => row.link.key);
  for (const [column] of TABLE_COLUMNS) for (const descending of [false, true]) {
    const sorted = sortTableRows([...rows].reverse(), column, descending);
    assert.deepEqual(sorted.map(row => row.link.key), sortTableRows(rows, column, descending).map(row => row.link.key));
  }
  for (const descending of [false, true]) assert.equal(sortTableRows(rows, 'confidence', descending).at(-1).confidence, null);
  assert.deepEqual(rows.map(row => row.link.key), original);
});

test('every semantic link stays inspectable, including self links and owner duplicates outside the Focus rings', () => {
  const custom = structuredClone(summary);
  custom.links[0].family = 'evidence';
  custom.links[1].neighbor.label = 'Document';
  custom.links[2].neighbor.id = 'root';
  custom.owner_duplicates = [{ id: custom.links[3].neighbor.id }];
  const rows = tableRows(custom, neighborhoodLayout(custom, 2));
  assert.equal(rows.length, 161);
  assert.ok(rows.some(row => row.link.key === custom.links[2].key && !row.onCanvas));
  assert.ok(rows.some(row => row.link.key === custom.links[3].key && !row.onCanvas));
  assert.ok(rows.every(row => !custom.links.slice(0, 2).some(link => link.key === row.link.key)));
});

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { parseFilters, filterLinks, filterUrl, ownerHidden } from '../.test-build/graph-filters.js';
const docs = { 1: { date: '2026-01-03', doc_type: 'insurance' }, 2: { date: '2025-12-01', doc_type: 'work' } };
const link = { key: 'a~HAS~b', family: 'parties', support_state: 'mixed', stated_count: 1, inferred_count: 1,
  neighbor: { id: 'b', is_owner: false }, records: [{ doc_id: 1, inferred: false, confidence: 0.9 }, { doc_id: 2, inferred: true, confidence: null }] };
const filters = query => parseFilters(new URLSearchParams(query));
test('document dates recompute mixed support, distinct documents and confidence', () => {
  const [result] = filterLinks([link], docs, filters('from=2026-01'));
  assert.equal(result.support_state, 'supported');
  assert.equal(result.stated_count, 1);
  assert.equal(result.inferred_count, 0);
  assert.equal(result.records.length, 1);
  assert.equal(result.records[0].confidence, 0.9);
  assert.equal(filterLinks([link], docs, filters('from=2027-01')).length, 0);
});
test('minimum confidence includes missing scores only with No score checked', () => {
  assert.equal(filterLinks([link], docs, filters('minconf=0.70&noscore=1')).length, 1);
  assert.equal(filterLinks([link], docs, filters('minconf=0.70&noscore=0')).length, 0);
  assert.equal(filterLinks([link], docs, filters('minconf=0.95&from=2026-01')).length, 0);
});
test('families, support, document minimum, types and owner filter combine', () => {
  for (const query of ['fam=money', 'fam=', 'support=inferred', 'mindocs=3', 'dtype=medical_lab'])
    assert.equal(filterLinks([link], docs, filters(query)).length, 0, query);
  assert.equal(filterLinks([link], docs, filters('dtype=insurance&support=supported&mindocs=1')).length, 1);
  assert.equal(filterLinks([{ ...link, neighbor: { id: 'b', is_owner: true } }], docs, filters('owner=hide')).length, 0);
});
test('cross-links use the same record filters and owner ids', () => {
  const { neighbor, ...rest } = link;
  assert.ok(neighbor);
  const cross = { ...rest, source: 'a', target: 'b' };
  assert.equal(filterLinks([cross], docs, filters('dtype=insurance'))[0].support_state, 'supported');
  assert.equal(filterLinks([cross], docs, filters('owner=hide'), new Set(['b'])).length, 0);
});
test('filter URL round trip preserves view, selection, cross-links and date range', () => {
  const current = new URLSearchParams('focus=a&view=table&sel=a~HAS~b&cross=0');
  const value = { ...filters('fam=money,parties&from=2026-01&to=2026-11&minconf=0.7&noscore=0&dtype=insurance&owner=hide'), minDocs: 3 };
  const url = new URL(filterUrl(current, value), 'https://example.test');
  assert.deepEqual(parseFilters(url.searchParams), value);
  for (const [key, value] of current) assert.equal(url.searchParams.get(key), value);
  assert.equal(filters('from=2026-13&to=bad&minconf=nan&mindocs=-1').from, '');
});
test('Owner defaults follow the view and explicit overrides survive switches', () => {
  for (const view of ['focus', 'timeline', 'table']) assert.equal(ownerHidden(filters(''), view), false);
  for (const view of ['explore', 'path', 'overview']) assert.equal(ownerHidden(filters(''), view), true);
  assert.equal(ownerHidden(filters('owner=show'), 'path'), false);
  assert.equal(ownerHidden(filters('owner=show'), 'overview'), true);
});

const { neighborhoodLayout, drawableLinks } = await import('../.test-build/graph-layout.js');
const { filterLayout } = await import('../.test-build/graph-filters.js');
function layoutSummary() {
  const links = ['parties', 'money', 'places'].flatMap((family, index) => Array.from({ length: 8 }, (_, i) => ({
    ...link, key: `focus~${family}~${index}-${i}`, type: family, family, direction: 'out',
    neighbor: { id: `${index}-${i}`, label: 'Organization', name: `Neighbor ${index}-${i}`, is_owner: false },
  })));
  return { focus: { id: 'focus', label: 'Person', name: 'Focus', is_owner: false }, links, documents: docs, owner_duplicates: [] };
}
test('filtering removes nodes and keeps every remaining node at its original position', () => {
  const summary = layoutSummary();
  const layout = neighborhoodLayout(summary, 6);
  const filtered = { ...summary, links: filterLinks(summary.links, docs, filters('fam=parties,places')) };
  const result = filterLayout(layout, filtered);
  const original = new Map(layout.nodes.map(node => [node.id, node]));
  assert.ok(result.nodes.length < layout.nodes.length);
  for (const node of result.nodes) assert.deepEqual([node.x, node.y], [original.get(node.id).x, original.get(node.id).y]);
  for (const sector of result.sectors) assert.equal(sector.drawn + sector.hidden, drawableLinks(filtered).filter(link => link.family === sector.family).length);
  assert.ok(result.nodes.every(node => node.family !== 'money'));
});
test('a retained secondary-family link keeps its neighbor in the original slot', () => {
  const summary = layoutSummary();
  summary.links.push({ ...summary.links[0], key: 'focus~SECOND~0-0', type: 'SECOND', family: 'money' });
  const layout = neighborhoodLayout(summary, 6);
  const filtered = { ...summary, links: filterLinks(summary.links, docs, filters('fam=money')) };
  const result = filterLayout(layout, filtered);
  assert.ok(result.links.some(link => link.key === 'focus~SECOND~0-0'));
  assert.deepEqual(result.nodes.find(node => node.id === '0-0'), layout.nodes.find(node => node.id === '0-0'));
});
test('Overview ignores the four disabled groups and always excludes owner endpoints', () => {
  const active = filters('from=2030-01&dtype=medical_lab&minconf=1&noscore=0&owner=show');
  assert.equal(filterLinks([link], docs, active, new Set(), 'overview').length, 1);
  assert.equal(filterLinks([{ ...link, neighbor: { is_owner: true } }], docs, active, new Set(), 'overview').length, 0);
});

test('Show more skips hidden links and reports only matching additions', async () => {
  const { showMoreFiltered } = await import('../.test-build/graph-filters.js');
  const raw = layoutSummary();
  const filtered = { ...raw, links: raw.links.filter(link => link.neighbor.id === '0-7') };
  const next = showMoreFiltered(raw, filtered, 2, {}, 'parties');
  assert.deepEqual(next, { parties: 6 });
  const result = filterLayout(neighborhoodLayout(raw, 2, next), filtered);
  assert.ok(result.nodes.some(node => node.id === '0-7'));
  assert.equal(result.sectors[0].hidden, 0);
});

test('large filtered groups use a bounded search and clearing filters keeps the entity cap', async () => {
  const { showMoreFiltered } = await import('../.test-build/graph-filters.js');
  const raw = layoutSummary();
  const links = Array.from({ length: 2000 }, (_, index) => ({ ...raw.links[0], key: `focus~HAS~large-${index}`,
    neighbor: { ...raw.links[0].neighbor, id: `large-${index}`, name: String(index).padStart(4, '0') } }));
  let reads = 0;
  Object.defineProperty(raw, 'links', { get() { reads++; return links; } });
  const filtered = { ...raw, links: [links[140]] };
  const next = showMoreFiltered(raw, filtered, 2, {}, 'parties');
  assert.ok(next.parties >= 139);
  assert.ok(neighborhoodLayout(raw, 2, next).entities <= 150, 'Clearing filters cannot reveal more than 150 entities');
  assert.ok(filterLayout(neighborhoodLayout(raw, 2, next), filtered).nodes.some(node => node.id === 'large-140'));
  assert.ok(reads < 100, `Bounded layout work: ${reads} reads of the 2,000-link collection`);
});

test('date filters use the parsed backend date and keep unparseable dates outside ranges', () => {
  const mixed = { 1: { date: 'January 3, 2026', parsed_date: '2026-01-03' }, 2: { date: '2026-01-01', parsed_date: null } };
  const [result] = filterLinks([link], mixed, filters('from=2026-01&to=2026-01'));
  assert.deepEqual(result.records.map(record => record.doc_id), [1]);
});

import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createRequire } from 'node:module';
const require = createRequire(import.meta.url);
const { documentMonth, timelineAxis, timelineMarks, timelineLanes, timelineRowName, timelineTracks, timelineLabels, inRange } = require('../.test-build/graph-timeline.js');
const neighbor = { id: 'empty1', name: '', label: 'Organization' };
const link = { key: 'a~COVERS~b', family: 'coverage', type: 'COVERS', neighbor, records: [1, 2, 3].map(doc_id => ({ doc_id, inferred: doc_id === 2 })), support_state: 'mixed', stated_count: 2, inferred_count: 1 };
const documents = { 1: { date: '2026-01-10' }, 2: { date: '2026-01-20' }, 3: { date: null } };
const summary = { focus: { id: 'a', is_hub: true }, links: [link], documents, owner_duplicates: [] };
test('each supporting document gets a dated or No date mark, with its support', () => {
  assert.deepEqual(timelineMarks(link, documents), [{ month: '2026-01', inferred: false, ids: [1] }, { month: '2026-01', inferred: true, ids: [2] }, { month: null, inferred: false, ids: [3] }]);
  assert.equal(documentMonth('January 10, 2026'), null);
  assert.equal(documentMonth('2026-13-01'), null);
  assert.deepEqual(timelineAxis(summary), ['2025-12', '2026-01', '2026-02']);
  assert.deepEqual(timelineAxis({ documents: { 1: { date: null } } }), []);
  assert.equal(inRange(null, '2026-01', ''), false);
  assert.equal(inRange('2026-01', '2026-01', '2026-01'), true);
});
test('busy links merge monthly marks without losing support or documents', () => {
  const records = Array.from({ length: 61 }, (_, i) => ({ doc_id: i + 1, inferred: i % 2 === 0 }));
  const docs = Object.fromEntries(records.map(r => [r.doc_id, { date: '2026-01-01' }]));
  const marks = timelineMarks({ ...link, records }, docs);
  assert.equal(marks.length, 1);
  assert.equal(marks[0].inferred, false);
  assert.equal(marks.flatMap(mark => mark.ids).length, 61);
});
test('hub caps rows per family and more adds 25, with stable single-family placement', () => {
  const links = Array.from({ length: 33 }, (_, i) => ({ ...link, key: String(i), neighbor: { ...neighbor, id: `b${i}` } }));
  assert.equal(timelineLanes({ ...summary, links })[0].rows.length, 2);
  assert.equal(timelineLanes({ ...summary, links })[0].hidden, 31);
  assert.equal(timelineLanes({ ...summary, links }, { coverage: 25 })[0].rows.length, 27);
  assert.match(timelineRowName(neighbor, [link], documents), /Organization empty1, outgoing covers link, 3 documents from January 2026 to January 2026, stated in 2, inferred in 1/);
});

test('dense dated and undated sources stay inside their columns without merging below 61 records', () => {
  const records = Array.from({ length: 30 }, (_, i) => ({ doc_id: i + 1, inferred: false }));
  const docs = Object.fromEntries(records.map(r => [r.doc_id, { date: '2026-01-01' }]));
  for (const documents of [docs, {}]) {
    const { tracks, height } = timelineTracks([{ ...link, records }], documents, 24);
    assert.equal(tracks[0].marks.length, 30);
    assert.ok(tracks[0].marks.every(mark => Math.abs(mark.dx) <= (mark.month ? 6 : 18)));
    assert.ok(height > 44);
  }
  assert.match(timelineRowName(neighbor, [{ ...link, direction: 'in' }], documents), /incoming covers link/);
  assert.equal(documentMonth('2026-01'), '2026-01');
});

test('year labels do not collide when the axis starts in December before a January source', () => {
  const months = Array.from({ length: 26 }, (_, i) => `${2023 + Math.floor((i + 11) / 12)}-${String((i + 11) % 12 + 1).padStart(2, '0')}`);
  const labels = timelineLabels(months, 424);
  assert.equal(labels[0].index, 1);
  assert.equal(labels[0].text, '2024');
  assert.ok(labels.every((label, i) => i === 0 || (label.index - labels[i - 1].index) / 25 * 324 >= 48));
  assert.match(timelineLabels(['2025-12', '2026-01', '2026-02'], 424)[0].text, /2025/);
});

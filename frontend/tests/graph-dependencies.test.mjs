import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

test('the graph dependencies exclude the 3D renderer and Three.js', async () => {
  for (const filename of ['package.json', 'package-lock.json']) {
    const manifest = JSON.parse(await readFile(new URL(`../${filename}`, import.meta.url), 'utf8'));
    const dependencies = manifest.packages ?? manifest.dependencies;
    assert.ok(Object.keys(dependencies).some(name => name.endsWith('react-force-graph-2d')));
    assert.ok(!Object.keys(dependencies).some(name => /(^|\/)(three|react-force-graph-3d)$/.test(name)));
  }
});

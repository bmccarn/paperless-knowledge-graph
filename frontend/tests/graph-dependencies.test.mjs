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

test('React lint versions match Next and keep the upgraded hooks rules', async () => {
  const manifest = JSON.parse(await readFile(new URL('../package.json', import.meta.url), 'utf8'));
  const lock = JSON.parse(await readFile(new URL('../package-lock.json', import.meta.url), 'utf8'));
  assert.equal(manifest.devDependencies['eslint-config-next'], manifest.dependencies.next);
  assert.equal(lock.packages['node_modules/eslint-config-next'].version, manifest.dependencies.next);
  assert.equal(lock.packages['node_modules/eslint-plugin-react-hooks'].version, '7.1.1');
});

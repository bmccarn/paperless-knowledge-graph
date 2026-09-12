"""Bind the actual local build, browser installation and Node executable."""
import hashlib
import json
import os
from pathlib import Path
import subprocess


def node_environment():
    """Node loaders and browser overrides cannot introduce unbound executable code."""
    allowed={'PATH','HOME','USER','LOGNAME','TMPDIR','TMP','TEMP','LANG','LC_ALL','LC_CTYPE','TZ','SystemRoot'}
    return {key:value for key,value in os.environ.items() if key in allowed}


def file_digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def tree_digest(root):
    root=Path(root)
    if not root.is_dir():raise ValueError('Required local runtime tree missing')
    files={}
    for path in sorted(root.rglob('*')):
        # Runtime response caches are not executable build inputs.
        if path.relative_to(root).parts[:2]==('.next','cache'):continue
        if path.is_symlink():
            target=path.resolve(strict=True)
            if not target.is_relative_to(root.resolve()):
                raise ValueError('Local runtime symlink escapes frozen tree')
            files[path.relative_to(root).as_posix()]='symlink:'+os.readlink(path)
            continue
        if path.is_file():files[path.relative_to(root).as_posix()]=file_digest(path)
    if not files:raise ValueError('Empty local runtime tree')
    return files


def local_identity(frontend, node):
    frontend=Path(frontend).resolve();node=Path(node).resolve()
    if frontend != Path(__file__).resolve().parents[1]/'frontend':
        raise ValueError('Browser and identity must use the same repository frontend')
    code="const {chromium}=require('playwright');process.stdout.write(JSON.stringify({executable:chromium.executablePath(),version:require('playwright/package.json').version}));"
    info=json.loads(subprocess.run([str(node),'-e',code],cwd=frontend,env=node_environment(),check=True,capture_output=True,timeout=15).stdout)
    executable=Path(info['executable']).resolve()
    install=next((p for p in executable.parents if p.name.startswith('chromium-')),None)
    if install is None:raise ValueError('Expected Playwright Chromium installation')
    source={}
    for name in ('src','public'):
        directory=frontend/name
        if directory.exists():source.update({name+'/'+key:value for key,value in tree_digest(directory).items()})
    for path in sorted(frontend.iterdir()):
        if path.is_file() and path.suffix in {'.json','.mjs','.js','.ts'}:
            source[path.name]=file_digest(path)
    return {'locale':{key:node_environment().get(key) for key in ('LANG','LC_ALL','LC_CTYPE','TZ')},'node_sha256':file_digest(node),'node_version':subprocess.run([str(node),'--version'],env=node_environment(),check=True,capture_output=True,timeout=15).stdout.decode().strip(),
            'frontend_source_sha256':source,'standalone_sha256':tree_digest(frontend/'.next/standalone'),
            'playwright_sha256':tree_digest(frontend/'node_modules/playwright'),
            'playwright_core_sha256':tree_digest(frontend/'node_modules/playwright-core'),
            'playwright_version':info['version'],'chromium_sha256':tree_digest(install),
            'chromium_executable':executable.relative_to(install).as_posix()}

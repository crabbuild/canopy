#!/usr/bin/env python3
"""Disposable native Git smoke fixture for the proposed storage contract.

Checks SHA-1/SHA-256, native pack/index installation, and thin-pack completion.
Does not exercise Canopy durability or simulate production scale.
"""
from pathlib import Path
import os
import shutil
import subprocess
import tempfile


def git(path, *args, data=None):
    env = {k:v for k,v in os.environ.items() if not k.startswith('GIT_')}
    env.update(GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL=os.devnull,
               GIT_AUTHOR_NAME='Fixture', GIT_AUTHOR_EMAIL='fixture@example.invalid',
               GIT_COMMITTER_NAME='Fixture', GIT_COMMITTER_EMAIL='fixture@example.invalid')
    return subprocess.run(['git','-C',str(path),*args],input=data,stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE,env=env,check=True,timeout=60).stdout


def run(fmt, root):
    source, target = root / 'source', root / 'target'
    source.mkdir(parents=True)
    target.mkdir()
    git(source,'init',f'--object-format={fmt}')
    git(target,'init','--bare',f'--object-format={fmt}')
    (source/'data').write_text('line\n'*10000)
    git(source,'add','data')
    git(source,'commit','-m','base')
    base=git(source,'rev-parse','HEAD').strip()
    prefix=target/'objects/pack/pack'
    checksum=git(source,'pack-objects','--revs','--index-version=2','--delta-base-offset',
                 '--threads=2','--window-memory=64m','--depth=50','--max-pack-size=1g',str(prefix),
                 data=base+b'\n').strip().decode()
    basepack=prefix.with_name('pack-'+checksum+'.pack')
    git(target,'index-pack','--verify',str(basepack))
    git(target,'update-ref','refs/heads/main',base.decode())
    (source/'data').write_text('line\n'*9999+'changed\n')
    git(source,'add','data')
    git(source,'commit','-m','next')
    tip=git(source,'rev-parse','HEAD').strip()
    thin=git(source,'pack-objects','--revs','--thin','--stdout',data=tip+b'\n^'+base+b'\n')
    git(target,'index-pack','--stdin','--fix-thin','--index-version=2',data=thin)
    git(target,'update-ref','refs/heads/main',tip.decode())
    git(target,'symbolic-ref','HEAD','refs/heads/main')
    git(target,'fsck','--full','--strict')
    expected=git(source,'rev-list','--objects','--all').splitlines()
    actual=git(target,'rev-list','--objects','--all').splitlines()
    assert sorted(expected)==sorted(actual)
    git(target,'multi-pack-index','write')
    git(target,'multi-pack-index','verify')
    git(target,'commit-graph','write','--reachable','--changed-paths')
    git(target,'commit-graph','verify')
    # Prove the completed thin pack can decode every physical object by itself.
    for pack in (target/'objects/pack').glob('*.pack'):
        isolated=root/pack.stem
        isolated.mkdir()
        git(isolated,'init','--bare',f'--object-format={fmt}')
        for ext in ('.pack','.idx'):
            shutil.copyfile(pack.with_suffix(ext),isolated/'objects/pack'/pack.with_suffix(ext).name)
        git(isolated,'index-pack','--verify',str(isolated/'objects/pack'/pack.name))
        ids=git(isolated,'cat-file','--batch-all-objects','--batch-check=%(objectname)')
        git(isolated,'cat-file','--batch',data=ids)
    print(f'{fmt}: pack/index, thin completion, isolated decode, MIDX, commit graph passed')


if __name__=='__main__':
    print(subprocess.check_output(['git','--version'],text=True).strip())
    with tempfile.TemporaryDirectory(prefix='canopy-pack-design-') as directory:
        for fmt in ('sha1','sha256'):
            run(fmt,Path(directory)/fmt)

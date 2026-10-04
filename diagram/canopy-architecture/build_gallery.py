#!/usr/bin/env python3
"""Build the offline HTML atlas from the diagrams and root design document."""
from pathlib import Path
from html import escape
import json
import re
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parent
CANOPY_REV = '9438bb865959fb975d5349ba8b9908b461653821'
CELLULE_REV = '161067f5a21703b3e257024bcb64e565fd9657b4'
diagrams = json.loads((ROOT/'manifest.json').read_text())
guide = (ROOT.parents[1]/'DESIGN.md').read_text()
sections = dict(section.split('\n', 1) for section in
                re.split(r'^## ', guide, flags=re.M)[1:])
design_sections = {
    '01-system-overview': 'System components',
    '02-canopy-on-cellule': 'Modeling Canopy on Cellule',
    '03-durable-command': 'Durable command execution',
    '04-request-routing': 'Request routing and residency',
    '05-git-push': 'Git push and replay',
    '06-read-and-lfs': 'Fetch browse and Git LFS',
    '07-owner-recovery': 'Owner recovery and operational control',
    '08-domain-and-policy': 'Collaboration and final policy checks',
    '09-packed-storage-evolution': 'Packed storage and framework capability evolution',
}
prose = {}
for slug, heading in design_sections.items():
    blocks = []
    for paragraph in sections[heading].split('\n\n'):
        paragraph = paragraph.strip()
        if not paragraph or paragraph.startswith(('![', '```')):
            continue
        if paragraph.startswith('|'):
            rows = [[escape(cell.strip()) for cell in row.strip().strip('|').split('|')]
                    for row in paragraph.splitlines()]
            header = '<thead><tr>'+''.join('<th>'+cell+'</th>' for cell in rows[0])+'</tr></thead>'
            body = '<tbody>'+''.join('<tr>'+''.join('<td>'+cell+'</td>' for cell in row)+'</tr>'
                                    for row in rows[2:])+'</tbody>'
            blocks.append('<div class="table-scroll"><table>'+header+body+'</table></div>')
        elif paragraph.startswith('### '):
            blocks.append('<h3>'+escape(paragraph[4:])+'</h3>')
        else:
            blocks.append('<p>'+escape(paragraph)+'</p>')
    prose[slug] = '\n'.join(blocks)

def inline_markup(value):
    value = re.sub(r'`([^`]+)`',r'<code>\1</code>',value)
    def link(match):
        destination = match.group(2)
        if not urlsplit(destination).scheme and not destination.startswith('//'):
            destination = ('../../DESIGN.md' if destination.startswith('#') else '../../') + destination
        return f'<a href="{destination}">{match.group(1)}</a>'
    return re.sub(r'\[([^\]]+)\]\(([^)]+)\)',link,value)

nav = ''.join(f'<a href="#{d["slug"]}"><span>{i:02}</span>{escape(d["title"])}</a>' for i,d in enumerate(diagrams,1))
articles = []
for i,d in enumerate(diagrams):
    slug = d['slug']
    svg = (ROOT/f'{slug}.svg').read_text()
    ids = re.findall(r'\bid="([^"]+)"',svg)
    for original in ids:
        svg = svg.replace(f'id="{original}"',f'id="{slug}-{original}"').replace(f'url(#{original})',f'url(#{slug}-{original})')
    svg = svg.replace('aria-labelledby="title desc"',f'aria-labelledby="{slug}-title {slug}-desc"')
    sources = ''.join(f'<li><a href="https://github.com/crabbuild/canopy/blob/{CANOPY_REV}/{s}">{escape(s)}</a></li>' for s in d['sources'])
    articles.append(f'''<section id="{slug}" class="chapter">
      <div class="chapter-head"><span class="ordinal">{i+1:02}</span><h2>{escape(d['title'])}</h2></div>
      <p class="caption">{escape(d['explanation'])}</p>
      <figure><div class="tools"><button type="button" data-action="out" aria-label="Zoom out">−</button><output>100%</output><button type="button" data-action="in" aria-label="Zoom in">+</button><button type="button" data-action="reset">Fit</button><span class="spacer"></span><a href="{slug}.svg" download>SVG</a><a href="{slug}@2x.png" download>PNG @2x</a></div><div class="viewport">{svg}</div></figure>
      <details><summary>Read the explanation and source code</summary><div class="explanation">{inline_markup(prose[slug])}</div><p><a href="../../DESIGN.md#{design_sections[slug].lower().replace(' ', '-')}">Read the complete design section</a></p><h3>Source map at the inspected revision</h3><ul class="sources">{sources}</ul></details>
    </section>''')

html = '''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Canopy architecture and Cellule integration</title>
<style>
:root{color-scheme:dark;--bg:#080f1e;--panel:#0f172a;--ink:#e2e8f0;--muted:#9eafc5;--cyan:#22d3ee;--border:#263449}*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;background:var(--bg);color:var(--ink);font:16px/1.65 system-ui,-apple-system,sans-serif}a{color:#74d8e9;text-underline-offset:4px}a:hover{color:#c8f7ff}button:focus-visible,a:focus-visible,summary:focus-visible{outline:3px solid var(--cyan);outline-offset:4px}.layout{max-width:1580px;margin:auto;display:grid;grid-template-columns:260px minmax(0,1fr);gap:38px;padding:40px 32px}aside{position:sticky;top:28px;align-self:start;height:calc(100vh - 56px);overflow:auto}.brand{font-weight:750;letter-spacing:.16em;color:var(--cyan);font-size:13px}.nav-title{font-size:13px;color:var(--muted);margin:20px 0 10px}nav a{display:flex;gap:12px;padding:11px 0;color:var(--muted);font-size:13px;text-decoration:none;line-height:1.4}nav span{color:#64748b;font-variant-numeric:tabular-nums;flex:0 0 20px}nav a:hover,nav a.active{color:var(--cyan)}main{min-width:0}header{padding:8px 0 28px}h1{font-size:clamp(28px,3.5vw,43px);line-height:1.16;letter-spacing:-.025em;margin:0 0 22px;max-width:750px}header p{max-width:900px;color:var(--muted)}.meta{font-size:13px}.status{padding:16px 20px;border-left:3px solid #fbbf24;background:#271d14;border-radius:0 8px 8px 0;color:#e0cb9f;font-size:14px;margin:26px 0}.overview{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin:25px 0 40px}.overview div{padding:18px;border:1px solid var(--border);border-radius:8px}.overview strong{display:block;font-size:14px;color:#f1f5f9}.overview span{display:block;font-size:13px;color:var(--muted);margin-top:5px}.chapter{scroll-margin-top:25px;border-top:1px solid var(--border);padding:32px 0 42px}.chapter-head{display:flex;gap:16px;align-items:baseline}.ordinal{color:var(--cyan);font:14px ui-monospace,monospace}h2{font-size:23px;line-height:1.3;font-weight:650;margin:0 0 14px}h3{font-size:15px}.caption{font-size:15px;color:var(--muted);max-width:920px;margin:0 0 24px}figure{margin:0;background:var(--panel);border:1px solid var(--border);border-radius:10px;overflow:hidden}.tools{display:flex;align-items:center;gap:12px;padding:12px 16px;border-bottom:1px solid var(--border);font-size:13px}.tools button{background:#172238;color:var(--ink);border:1px solid #35445e;border-radius:5px;padding:5px 11px;cursor:pointer;font:inherit}.tools button:hover{border-color:var(--cyan)}output{min-width:40px;text-align:center;color:var(--muted);font-variant-numeric:tabular-nums}.spacer{flex:1}.viewport{overflow:auto;max-height:1050px;scrollbar-color:#475569 #0f172a}.viewport svg{display:block;width:100%;max-width:none;height:auto}details{margin:20px 0 0;font-size:14px}summary{cursor:pointer;color:var(--cyan);padding:10px 0}.explanation{max-width:880px;color:#c2ccda}.explanation p{margin:14px 0}code{font:12px/1.5 ui-monospace,monospace;background:#1b293e;padding:2px 4px;border-radius:3px;overflow-wrap:anywhere}.sources{font:12px/1.7 ui-monospace,monospace;padding-left:20px;overflow-wrap:anywhere}.sources li{margin:6px 0}footer{padding:25px 0 40px;color:var(--muted);font-size:13px}@media(max-width:1000px){.layout{grid-template-columns:1fr;padding:25px 18px;gap:15px}aside{position:static;height:auto}.nav-title,nav{display:none}header{padding-top:0}.overview{grid-template-columns:1fr}.viewport{max-height:850px}}@media(prefers-reduced-motion:reduce){html{scroll-behavior:auto}}@media print{aside,.tools{display:none}.layout{display:block;padding:0}.chapter{break-before:page}.viewport{max-height:none;overflow:visible}details .explanation{display:block}body{background:white;color:black}header p,.caption{color:#334155}}
.table-scroll{overflow-x:auto;margin:18px 0}.explanation table{width:100%;border-collapse:collapse;font-size:13px;text-align:left}.explanation th,.explanation td{padding:10px 12px;border:1px solid var(--border);vertical-align:top;min-width:130px}.explanation th{background:#172238;color:#e2e8f0;font-weight:600}
</style></head><body><div class="layout"><aside><div class="brand">CANOPY / CELLULE</div><div class="nav-title">Architecture atlas · 9 diagrams</div><nav>'''+nav+'''</nav><p class="meta"><a href="../../DESIGN.md">Design documentation</a></p></aside><main>
<header><h1>Canopy architecture and Cellule integration</h1><p>Trace a request from a Git client or browser to its authoritative repository state. See which components Canopy owns, what Cellule supplies, and how writes survive owner changes and local disk loss.</p>
<p class="meta">Source snapshot: October 4, 2026 · Canopy <a href="https://github.com/crabbuild/canopy/tree/'''+CANOPY_REV+'''">9438bb8</a> · Cellule <a href="https://github.com/crabbuild/cellule/tree/'''+CELLULE_REV+'''">161067f</a></p>
<div class="status">Diagrams 01–08 describe the current serving architecture. Diagram 09 separates newer implemented packed-storage primitives from incomplete production integration. Composed repository queues and workflows remain a separate proposal. These diagrams do not assert production capacity.</div>
<div class="overview"><div><strong>Product boundary</strong><span>Canopy owns ingress, Git, auth, collaboration and operational policy.</span></div><div><strong>Framework boundary</strong><span>Cellule supplies stable targets, fenced execution, durable outcomes and restoration.</span></div><div><strong>Authority boundary</strong><span>Cells and published roots preserve state. Native Git files are rebuildable.</span></div></div>
<p class="meta">Use + and − to inspect a diagram; drag the scrollbar to pan. Fit restores the overview. Each figure has standalone SVG and PNG exports and expandable explanations with source links.</p></header>
'''+''.join(articles)+'''<footer>Created with the baoyu-diagram skill. SVGs contain their own styles and use system fonts. The gallery embeds all nine figures and works offline; source links require network access.<br><a href="../../DESIGN.md">Read the full design</a> · <a href="generate.py">Diagram generator</a> · <a href="manifest.json">Source manifest</a></footer></main></div>
<script>
document.querySelectorAll('figure').forEach(figure=>{let zoom=100;const svg=figure.querySelector('svg'), output=figure.querySelector('output');figure.querySelectorAll('button').forEach(button=>button.addEventListener('click',()=>{zoom=button.dataset.action==='reset'?100:Math.min(250,Math.max(50,zoom+(button.dataset.action==='in'?25:-25)));svg.style.width=zoom+'%';output.value=zoom+'%';}));});
if('IntersectionObserver' in window){const observer=new IntersectionObserver(entries=>entries.forEach(entry=>{if(entry.isIntersecting){document.querySelectorAll('nav a').forEach(a=>a.classList.toggle('active',a.hash==='#'+entry.target.id));}}),{rootMargin:'-5% 0px -65% 0px'});document.querySelectorAll('.chapter').forEach(chapter=>observer.observe(chapter));}
</script></body></html>'''
(ROOT/'index.html').write_text(html)
print(f'Built self-contained gallery with {len(diagrams)} diagrams')

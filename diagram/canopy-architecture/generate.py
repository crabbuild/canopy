#!/usr/bin/env python3
"""Regenerate the source-based Canopy architecture atlas (stdlib only)."""
from pathlib import Path
from html import escape as esc
import json
import textwrap

OUT = Path(__file__).resolve().parent
COLORS = {
    'cyan': ('#083344', '#22d3ee'), 'green': ('#064e3b', '#34d399'),
    'violet': ('#4c1d95', '#a78bfa'), 'amber': ('#78350f', '#fbbf24'),
    'rose': ('#881337', '#fb7185'), 'orange': ('#7c2d12', '#fb923c'),
    'slate': ('#1e293b', '#94a3b8'), 'blue': ('#1e3a8a', '#60a5fa'),
}
ATLAS = []

class Diagram:
    def __init__(self, slug, title, subtitle, height=850, width=1100, status='CURRENT SERVING PATH', sequence=False):
        self.slug, self.title, self.subtitle = slug, title, subtitle
        self.w, self.h, self.status = width, height, status
        self.sequence = sequence
        self.regions, self.edges, self.nodes, self.labels = [], [], [], []
        self.activations, self.lifelines = [], []
        self.foreground_edges = []
        self.boxes = []

    def txt(self, x, y, value, size=9, color='#94a3b8', anchor='start', weight=400, layer=None):
        (self.labels if layer is None else layer).append(
            f'<text x="{x}" y="{y}" fill="{color}" font-size="{size}" text-anchor="{anchor}" font-weight="{weight}">{esc(value)}</text>')

    def region(self, x, y, w, h, label, color='amber'):
        self.regions.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="12" fill="none" stroke="{COLORS[color][1]}" stroke-dasharray="8 4"/>')
        self.txt(x+16, y+20, label, color=COLORS[color][1], weight=600, layer=self.regions)

    def box(self, x, y, w, h, title, lines=(), color='green', dashed=False):
        fill, stroke = COLORS[color]
        self.boxes.append((x,y,w,h,title))
        self.nodes.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="7" fill="#0f172a"/>')
        self.nodes.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="7" fill="{fill}" fill-opacity=".4" stroke="{stroke}" stroke-width="1.4"'+(' stroke-dasharray="5 4"' if dashed else '')+'/>')
        titles = title if isinstance(title, list) else [title]
        ytext = y+25
        for line in titles:
            assert len(line)*7.2 <= w-18, (self.slug, title, 'title too wide')
            self.txt(x+w/2, ytext, line, 12, '#f1f5f9', 'middle', 600)
            ytext += 17
        if lines:
            ytext += 3
        for line in lines:
            assert len(line)*5.4 <= w-18, (self.slug, line, 'body too wide')
            self.txt(x+w/2, ytext, line, 9, '#b4c1d3', 'middle')
            ytext += 15
        assert ytext-15 <= y+h-10, (self.slug, title, 'text too tall')

    def note(self, x, y, w, title, text, color='slate', h=None):
        lines = textwrap.wrap(text, int((w-30)/5.4), break_long_words=False, break_on_hyphens=False)
        self.box(x,y,w,h or 48+15*len(lines),title,lines,color)

    def path(self, points, label=None, color='slate', dashed=False, lx=None, ly=None, foreground=False):
        stroke = COLORS[color][1]
        d = 'M '+' L '.join(f'{x},{y}' for x,y in points)
        # Every sequence message belongs above both activation bars and lifelines.
        layer = self.foreground_edges if self.sequence or foreground else self.edges
        layer.append(f'<path d="{d}" fill="none" stroke="{stroke}" stroke-width="1.5"'+(' stroke-dasharray="5 4"' if dashed else '')+f' marker-end="url(#arrow-{color})"/>')
        if label:
            self.txt(lx if lx is not None else (points[0][0]+points[-1][0])/2,
                     ly if ly is not None else (points[0][1]+points[-1][1])/2-9,
                     label, 8, stroke, 'middle')

    def lifeline(self, x, start, end):
        self.lifelines.append(f'<path class="sequence-lifeline" d="M{x} {start} V{end}" stroke="#334155" stroke-dasharray="6 5"/>')

    def activation(self, x, y, height, color):
        self.activations.append(f'<rect class="sequence-activation" x="{x-5}" y="{y}" width="10" height="{height}" fill="{COLORS[color][0]}" stroke="{COLORS[color][1]}"/>')

    def diamond(self, cx, cy, w, h, lines):
        self.nodes.append(f'<polygon points="{cx},{cy-h/2} {cx+w/2},{cy} {cx},{cy+h/2} {cx-w/2},{cy}" fill="#0f172a"/>')
        self.nodes.append(f'<polygon points="{cx},{cy-h/2} {cx+w/2},{cy} {cx},{cy+h/2} {cx-w/2},{cy}" fill="#78350f" fill-opacity=".4" stroke="#fbbf24" stroke-width="1.4"/>')
        for i,line in enumerate(lines):
            self.txt(cx,cy+(i-(len(lines)-1)/2)*15+4,line,10,'#f1f5f9','middle',600)

    def legend(self, entries, y=None):
        y = y or self.h-40
        x = 35
        for color,label in entries:
            self.labels.append(f'<circle cx="{x+4}" cy="{y-3}" r="4" fill="{COLORS[color][1]}"/>')
            self.txt(x+16,y,label,8)
            x += len(label)*4.8+46

    def save(self, explanation, sources):
        for x,y,w,h,t in self.boxes:
            assert x>=30 and y>=85 and x+w<=self.w-30 and y+h<=self.h-60, (self.slug,t,'outside drawing area')
        markers = ''.join(f'<marker id="arrow-{c}" markerWidth="8" markerHeight="6" refX="7" refY="3" orient="auto"><path d="M0 0 L8 3 L0 6" fill="{p[1]}"/></marker>' for c,p in COLORS.items())
        heading = f'<text x="30" y="33" font-size="16" font-weight="700" fill="#f8fafc">{esc(self.title)}</text><text x="30" y="56" font-size="9" fill="#94a3b8">{esc(self.subtitle)}</text><text x="{self.w-30}" y="33" font-size="8" text-anchor="end" fill="#fbbf24">{esc(self.status)}</text>'
        svg = f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {self.w} {self.h}" role="img" aria-labelledby="title desc">
<title id="title">{esc(self.title)}</title><desc id="desc">{esc(explanation)}</desc>
<defs><style>text {{font-family: 'SF Mono', 'Cascadia Code', 'DejaVu Sans Mono', monospace;}}</style><pattern id="grid" width="40" height="40" patternUnits="userSpaceOnUse"><path d="M40 0 H0 V40" fill="none" stroke="#1e293b" stroke-width=".5"/></pattern>{markers}</defs>
<rect width="100%" height="100%" fill="#0f172a"/><rect width="100%" height="100%" fill="url(#grid)"/>
{''.join(self.regions)}{''.join(self.edges)}{''.join(self.activations)}{''.join(self.nodes)}{''.join(self.lifelines)}{''.join(self.foreground_edges)}{''.join(self.labels)}{heading}
</svg>'''
        (OUT/f'{self.slug}.svg').write_text(svg)
        ATLAS.append(dict(slug=self.slug,title=self.title,explanation=explanation,sources=sources))


# 1. Product components and storage authority.
d = Diagram('01-system-overview','Canopy system architecture','Clients enter any node; Cell authority and immutable storage preserve repository state.', 900)
d.region(230,95,465,550,'CANOPY NODE / ONE RUST SERVICE')
d.region(735,95,335,330,'DURABLE CELL AUTHORITY','violet')
d.box(35,145,155,75,'Git clients',['Smart HTTP / SSH','clone, fetch, push'],'cyan')
d.box(35,285,155,75,'Browser / API',['Embedded web UI','JSON collaboration API'],'cyan')
d.box(35,425,155,75,'Git LFS',['HTTP bodies / locks','SSH can issue grants'],'cyan')
d.box(255,145,185,90,'Ingress',['Axum HTTP router','Optional russh listener','Account authentication'],'cyan')
d.box(480,145,185,90,'RepositoryManager',['Name resolution','Residency + request pins','Local / peer Cell handles'],'orange')
d.box(255,290,410,100,'Product services',['GitGateway + LfsService + repository_http','Browse, issues, PRs, reviews, checks, policy','Native Git work runs on the receiving node'])
d.box(255,450,410,100,'Cellule integration',['CanopyApplication + CellNode','Typed SQL / domain commands','Fenced ownership + durable output gate'],'blue')
d.box(765,145,275,95,'Directory Cell',['One fixed SQL shard per tenant','Names -> stable repository UUID','Accounts, tokens, keys, audit'],'violet')
d.box(765,300,275,95,'Repository Cell',['One SQL Cell per repository UUID','Refs, objects, ACL, collaboration','Outcomes, closure and policy'],'violet')
d.box(765,485,275,100,'Disposable Git cache',['Private refs + verified shared objects','Native receive/upload-pack workers','Can be rebuilt after local disk loss'],'slate',True)
d.box(255,685,785,100,'Shared S3-compatible object store',['Cellule: catalog, authority, node leases, LTX roots and immutable SQLite recovery bytes','Canopy: external Git blobs, archived pack/index bytes and Git LFS bodies','Conditional writes select authority; verified immutable bytes supply recovery'],'violet')
d.path([(190,182),(255,182)],color='cyan')
d.path([(190,322),(212,322),(212,200),(255,200)],color='cyan')
d.path([(190,462),(220,462),(220,215),(255,215)],color='cyan')
d.path([(440,185),(480,185)],color='orange')
d.path([(665,175),(765,175)],'resolve / auth','violet')
d.path([(665,205),(714,205),(714,345),(765,345)],color='violet')
d.path([(575,235),(575,290)],color='green')
d.path([(460,390),(460,450)],'typed operations','blue')
d.path([(665,340),(715,340),(715,525),(765,525)],'native work','slate',lx=718,ly=453)
d.path([(665,500),(705,500),(705,375),(765,375)],color='blue')
d.path([(450,550),(450,685)],'durable publication / external body I/O','violet',lx=520,ly=628)
d.path([(1000,395),(1000,440),(1057,440),(1057,650),(850,650),(850,685)],color='violet')
d.path([(922,395),(922,445),(902,445),(902,485)],'hydrate','slate',True,lx=958,ly=460)
d.note(35,690,175,'Ownership rule','Git success is acknowledged only after the Cell commits and passes its durability gate.', 'amber')
d.legend([('cyan','public clients'),('green','product logic'),('blue','framework'),('violet','durable authority'),('slate','rebuildable local files')])
d.save('Canopy is an embedded Cellule application. Public ingress and native Git run in Canopy, while each durable Directory or Repository Cell has one fenced owner. A receiving node may call a remote owner. The object store holds authority records, SQLite recovery roots and product body objects. Native Git caches are disposable.', ['crates/canopy-server/src/server/mod.rs','crates/canopy-server/src/server/residency/mod.rs','crates/canopy-server/src/git_gateway/mod.rs','crates/canopy-server/src/lib.rs','crates/canopy-server/src/schema.sql'])

# 2. Exact mapping from Canopy domain to framework topology.
d=Diagram('02-canopy-on-cellule','How Canopy models its domain on Cellule','Domain code supplies schema and policy; the embedded framework supplies identity, execution and recovery.',1020)
d.region(35,95,1030,185,'APPLICATION DECLARATION / COMPILED AT STARTUP','green')
d.box(60,140,260,105,'CanopyApplication',['CellApplication::register','DirectoryModule + RepositoryModule','Two declared SQL Cell types'])
d.box(360,140,315,105,'DirectoryModule',['CatalogRole::Sql / one fixed shard','Namespace = [0x48; 16]','Names, accounts, credentials'])
d.box(715,140,325,105,'RepositoryModule',['CatalogRole::Sql / entity partitions','Namespace = [0x47; 16]','Git + ACL + collaboration commands'])
d.region(35,330,1030,190,'RUNTIME ADDRESSING / STABLE ACROSS OWNER MOVEMENT','blue')
d.box(60,380,410,105,'Directory target',['Tenant + application + namespace','partition_for_shard(0)','DirectoryCell wraps SqlCell<DirectoryModule>'],'blue')
d.box(545,380,495,105,'Repository target',['Tenant + application + namespace + UUID-derived partition','CellType::entity_partition -> 33-byte partition','RepositoryCell wraps SqlCell<RepositoryModule>'],'blue')
d.path([(518,245),(518,310),(265,310),(265,380)],color='blue')
d.path([(875,245),(875,380)],'UUID selects one Cell','blue',lx=949,ly=312)
d.path([(265,485),(265,605)],color='blue')
d.path([(795,485),(795,540),(545,540),(545,605)],color='blue')
d.region(35,560,1030,230,'EMBEDDED CELLULE FRAMEWORK / THE PINNED DEPENDENCY','amber')
d.box(60,605,290,95,'cellule-app + host',['Descriptor / registry / typed handles','CellNode lifecycle + readiness','Canopy installs facilities and leases'],'blue')
d.box(390,605,310,95,'cellule-runtime',['Per-Cell actor + SQL worker','Authority fence + request ledger','Queries, commands, peer contracts'],'orange')
d.box(740,605,300,95,'cellule-ltx + store',['Managed SQLite WAL capture','Verified roots + exact restoration','Bounded I/O + conditional updates'],'violet')
d.path([(350,650),(390,650)],color='orange')
d.path([(700,650),(740,650)],color='violet')
d.txt(60,746,'Dependency pin: 161067f5a21703b3e257024bcb64e565fd9657b4',9,'#e2e8f0')
d.txt(60,767,'cellule-types is transitive. Canopy supplies its own signed HTTPS peer transport.',9)
d.note(35,840,490,'Framework capabilities','Cellule includes SQL, KV, Queue, Workflow, Blob, Cron, Timer and Effects. Capabilities use typed APIs and explicit topology.', 'blue',115)
d.note(575,840,490,'Canopy capability boundary','Canopy registers SQL Cells today. Multi-capability Repository Cells, repository queues and autonomous workflows remain a proposal.', 'amber',115)
d.legend([('green','domain declarations'),('blue','framework binding'),('orange','fenced execution'),('amber','capabilities not wired into Canopy')])
d.save('CanopyApplication registers DirectoryModule and RepositoryModule as SQL Cell types. Directory uses a fixed shard; repository UUIDs derive entity partitions. Product wrappers hold typed SqlCell handles. Cellule owns execution and persistence mechanics, and Canopy owns protocols and authorization. Other framework primitives are available but are not composed into Canopy Repository Cells.', ['crates/canopy-server/src/lib.rs','crates/canopy-server/src/directory/mod.rs','crates/canopy-server/Cargo.toml','docs/repository-cell-primitives.md'])

# 3. Framework command execution and the real acknowledgement boundary.
d=Diagram('03-durable-command','Cellule command execution and durability','One command changes one Cell. The domain mutation and its recorded answer share a SQLite transaction.',1040,width=1140,sequence=True)
xs=[115,340,565,795,1025]
actors=[('Canopy caller','Typed handle','cyan'),('Cell owner','Actor + fence','orange'),('Managed SQLite','State + ledger','green'),('LTX / object store','Immutable roots','violet'),('CellAuthority','Control CAS','amber')]
for x,(t,s,c) in zip(xs,actors):
    d.box(x-85,110,170,65,t,[s],c)
    d.lifeline(x,175,835)
for x,y,h,c in [(340,220,550,'orange'),(565,370,95,'green'),(795,510,95,'violet'),(1025,650,65,'amber')]:
    d.activation(x,y,h,c)
msgs=[(0,1,225,'1  Command + stable MutationIdentity','cyan',False),
      (1,4,290,'2  Validate owner incarnation / lease / compatible code','amber',False),
      (1,2,375,'3  Execute domain mutation + request outcome in one transaction','green',False),
      (2,1,445,'4  Commit SQLite and capture its exact WAL boundary','green',True),
      (1,3,525,'5  Verify capture; upload immutable chunks and proposed root','violet',False),
      (3,1,595,'6  Return proposed recovery root','violet',True),
      (1,4,660,'7  Conditional publish of the exact root under owner fence','amber',False),
      (4,1,730,'8  Accepted authority revision = durable publication proof','amber',True),
      (1,0,795,'9  Committed<Output> + Receipt','blue',True)]
# Sequence layers: activation bars, dashed lifelines, then message arrows.
for a,b,y,l,c,ret in msgs:d.path([(xs[a],y),(xs[b],y)],l,c,ret)
d.note(35,865,490,'Lost reply or rejected fence','A timeout does not prove failure. Resolve the original request identity; retry the same logical command only under its existing identity.', 'rose',110)
d.note(575,865,490,'Receipt and scope','A receipt-bound query must observe the published per-Cell position. Directory and Repository commands do not form one cross-Cell SQL transaction.', 'blue',110)
d.legend([('cyan','invocation'),('green','local transaction'),('violet','immutable bytes'),('amber','authority publication'),('blue','durable acknowledgement')])
d.save('This is the object-store durability path assembled by Canopy. The Cell owner records domain state and the request outcome together, captures SQLite through LTX and publishes the exact root using a fenced conditional authority update. Only then does the caller receive Committed and a receipt. A Cellule follower-log mode exists, but this diagram does not claim Canopy enables it.', ['crates/canopy-server/src/server/mod.rs','crates/canopy-server/src/lib.rs'],)

# 4. Request routing with admission and takeover branches.
d=Diagram('04-request-routing','Request routing and repository residency','Authentication, name lookup, account admission and owner resolution precede repository work.',1190)
d.box(400,105,300,65,'Incoming request',['HTTP / SSH / API / Git LFS'],'cyan')
d.box(400,230,300,85,'Directory Cell',['Validate token / key / LFS grant','Resolve ready owner/name -> UUID'],'violet')
d.box(400,375,300,85,'RepositoryManager',['Derive target; pin resident route','Otherwise admit a transition'],'orange')
d.diamond(550,575,210,85,['Live owner','elsewhere?'])
d.box(60,695,350,100,'Remote Cell binding',['CellClient::peer -> /internal/cell','Signed request + enrolled node key','TLS, release, expiry, principal checks'],'blue')
d.box(690,695,350,100,'Local acquisition',['Bootstrap new / acquire idle Cell','Expired owner: fence + restore exact root','CellClient::local -> resident actor'],'blue')
d.box(400,900,300,95,'Repository operations',['Recheck current ACL / visibility','Run domain command or native Git','Retain route pin through response'])
d.path([(550,170),(550,230)],color='cyan')
d.path([(550,315),(550,375)],color='violet')
d.path([(550,460),(550,532)],color='orange')
d.path([(445,575),(235,575),(235,695)],'Yes','blue',lx=330,ly=565)
d.path([(655,575),(865,575),(865,695)],'No','blue',lx=758,ly=565)
d.path([(235,795),(235,850),(485,850),(485,900)],color='blue')
d.path([(865,795),(865,850),(615,850),(615,900)],color='blue')
d.note(35,230,285,'Discovery is a hint','Directory listing candidates are checked against current Repository Cell access. Cached names and UI state cannot grant permissions.', 'slate',135)
d.note(785,375,280,'Bounded transitions','Cold/remote work uses supervised admission and a per-repository transition lock. Different repositories can activate concurrently.', 'amber',135)
d.note(35,1040,490,'Capacity and cancellation','An unavailable slot or in-progress movement can return 503. Streamed responses and detached admitted work retain pins until they finish.', 'amber',85)
d.note(575,1040,490,'Native work placement','Remote binding moves Cell calls. Git workers, body streaming and the disposable cache can remain on the receiving gateway.', 'slate',85)
d.legend([('cyan','request'),('violet','identity / authority'),('orange','admission'),('blue','local or signed remote execution')])
d.save('Directory state authenticates accounts and resolves ready repository names. RepositoryManager binds a stable target to either a local actor or a signed peer client. Cold transitions are admitted and supervised; remote-owner loss triggers acquisition on demand. Repository authorization is checked using current durable state. Response pins prevent eviction during streaming.', ['crates/canopy-server/src/server/residency/mod.rs','crates/canopy-server/src/server/peer.rs','crates/canopy-server/src/repository_http/authorization.rs','crates/canopy-server/src/server/discovery.rs'])

# 5. Production receive-pack sequence, including archives on the serving path.
d=Diagram('05-git-push','Git push from wire input to durable refs','Current serving path: native Git prepares private state; CompletePush publishes refs and the saved result.',1300,width=1140,sequence=True)
xs=[110,335,560,800,1030]
for x,(t,s,c) in zip(xs,[('Git client','receive-pack','cyan'),('GitGateway','Receiving node','green'),('Native Git','Disposable refs','slate'),('Repository Cell','Via local / peer client','blue'),('Object store','Verified body bytes','violet')]):
    d.box(x-80,110,160,65,t,[s],c)
    d.lifeline(x,175,1045)
msgs=[(0,1,225,'1  Spool encoded input; bind push UUID + actor + request digest','cyan',False),
      (1,3,285,'2  begin_push: claim logical ID or find completed response','blue',False),
      (3,1,345,'3  Completed ID replays saved result; fresh ID continues','blue',True),
      (1,3,405,'4  Read consistent refs, generation and branch policy','blue',False),
      (1,2,465,'5  Decode / validate; receive-pack in private cache','green',False),
      (2,1,525,'6  Native report + actual accepted ref differences','slate',True),
      (1,4,585,'7  Upload verified external blobs or archived pack/index bytes','violet',False),
      (1,3,645,'8  Persist canonical object records in bounded batches','blue',False),
      (1,3,705,'9  Certify typed graph closure; stage response + ref plan','blue',False),
      (1,3,765,'10  CompletePush: recheck ACL, branch rules, OIDs and versions','green',False),
      (3,4,835,'11  Cellule publishes SQLite root','violet',False),
      (4,3,895,'12  Fenced durable proof','violet',True),
      (3,1,955,'13  Read canonical saved response','blue',True),
      (1,0,1015,'14  Git report + push identity','cyan',True)]
for a,b,y,l,c,ret in msgs:d.path([(xs[a],y),(xs[b],y)],l,c,ret)
d.region(35,720,1030,100,'FINAL DOMAIN TRANSACTION: ACCEPTED REFS + GENERATION + OUTCOME POINTER / AUDIT','green')
d.note(35,1080,490,'Two identities matter','The HTTP push UUID identifies the whole wire operation. Cellule MutationIdentity identifies each durable command within that operation.', 'amber',125)
d.note(575,1080,490,'Failure behavior','Preparation changes disposable refs only. Objects may remain unreferenced after refusal. An uncertain final publication is resolved, never turned into a false negative report.', 'rose',125)
d.txt(35,1230,'Native partial acceptance is preserved; Git --atomic can request all-or-none validation. Current gateway push work uses a mutex.',9)
d.legend([('cyan','wire protocol'),('slate','private native state'),('blue','durable Cell calls'),('violet','body / root publication')])
d.save('After outer authentication, GitGateway spools and hashes the encoded request and binds a logical push ID to the actor and digest. It can replay a completed push before decoding and native work. New attempts prepare a private ref snapshot, run receive-pack, ingest canonical objects, certify graph closure and stage the report and ref plan. CompletePush checks current policy and expected ref versions and commits accepted refs with the canonical response pointer. Cellule gates acknowledgement on durable publication.', ['crates/canopy-server/src/git_gateway/mod.rs','crates/canopy-server/src/git_gateway/preflight.rs','crates/canopy-server/src/git_gateway/push.rs','crates/canopy-server/src/push/mod.rs','crates/canopy-server/src/refs.rs'])

# 6. Separate read and LFS paths, plus exact physical storage choices.
d=Diagram('06-read-and-lfs','Fetch, browse and Git LFS data paths','Reads verify identities against Cell metadata. LFS body delivery bypasses native Git.',1220)
d.region(35,95,1030,330,'GIT CLONE / FETCH','cyan')
d.box(60,150,270,85,'Authorize and select',['Current ACL / public visibility','Validate wants against live refs'],'cyan')
d.box(390,150,310,85,'Snapshot and hydrate',['Generation-consistent refs + HEAD','Verify required objects / bodies','Apply supported partial-clone filter'])
d.box(760,150,280,85,'Native upload-pack',['Private ref snapshot','Shared verified object files','Stream generated pack to client'],'slate')
d.path([(330,192),(390,192)],color='cyan')
d.path([(700,192),(760,192)],color='green')
d.note(60,290,440,'Discovery fast path','Git v2 capability discovery needs no object hydration. Ref discovery prepares ref targets and annotated tag chains.', 'blue',95)
d.note(570,290,470,'Selective fetch','Blobless fetch omits ordinary blobs. Selected wants, ref targets and structural history determine hydration; warm verified objects are reused.', 'slate',95)
d.region(35,475,1030,245,'BROWSE / JSON READS / LFS','green')
d.box(60,530,280,115,'Repository browser',['Read trees, blobs, history and diffs','Use Repository Cell metadata','Load bodies from verified readers','No receive-pack is needed'])
d.box(400,530,280,115,'LFS upload',['Authorize batch / PUT','Hash SHA-256 + bounded parts','Publish immutable body, verify','Then commit authorized metadata'],'violet')
d.box(740,530,300,115,'LFS download',['Authorize against current Cell','Read manifest pinned in SQLite','Verify requested parts / digests','Support tail Range / 206 response'],'violet')
d.txt(60,689,'LFS locks are advisory records in the Repository Cell. SSH grants still deliver LFS bytes over HTTP.',9)
d.region(35,770,1030,270,'CURRENT SERVING STORAGE / AUTHORITATIVE METADATA REMAINS PER OBJECT','violet')
d.box(60,825,290,155,'Repository SQLite',['objects: kind, size, digest, storage','inline bodies <= 768 KiB','Large non-blob bodies: SQL chunks','refs + graph edges / certificates','LFS metadata + locks'],'violet')
d.box(390,825,310,155,'Immutable external bytes',['Large loose Git blobs + manifests','Archived pack and index bodies','Packed blobs reference approved packs','Git LFS bodies + part digests','Readers verify canonical identities'],'violet')
d.box(740,825,300,155,'Local cache',['Hydrated Git files / installed packs','Insertion cursor + ref generations','Private native process workspace','Disk and native-worker admission','Disposable after recovery'],'slate',True)
d.path([(350,903),(390,903)],'references','violet')
d.path([(700,903),(740,903)],'verify / hydrate','slate',True)
d.txt(60,1014,'This active archive path differs from the future immutable catalog / ref-root hard cutover in diagram 09.',9,'#fbbf24')
d.note(35,1080,1030,'Read authority','An uploaded pack, cached object, stale listing or UI view cannot independently authorize an object read. Current Cell access and verified published metadata govern visibility.', 'amber',65)
d.legend([('cyan','stock Git reads'),('green','browse / selection'),('violet','durable state and bodies'),('slate','rebuildable work files')],y=1180)
d.save('Fetch takes consistent refs, validates requested object reachability, hydrates selected verified objects and uses native upload-pack to stream the wire response. Browser APIs read through Cell metadata and verified body readers. LFS uploads first publish and verify immutable bytes, then commit authorized metadata; downloads verify manifest-bound parts. The active serving schema supports inline, chunked, external and packed storage records, distinct from the incomplete immutable catalog replacement.', ['crates/canopy-server/src/git_gateway/fetch.rs','crates/canopy-server/src/git_gateway/hydration.rs','crates/canopy-server/src/git_gateway/discovery.rs','crates/canopy-server/src/git_read/mod.rs','crates/canopy-server/src/lfs/upload.rs','crates/canopy-server/src/lfs/read.rs','crates/canopy-server/src/schema.sql'])

# 7. Lifecycle controls, fencing and exact restoration.
d=Diagram('07-owner-recovery','Owner lifecycle and exact recovery','A repository keeps its identity when its owner changes. Only authority can select the recovery root.',1150)
d.region(35,95,1030,180,'STARTUP AND SERVING','amber')
d.box(60,140,290,95,'Startup preflight',['Lock managed local workspace','Probe conditional writes / ranged I/O','Compile and check selected release'],'amber')
d.box(390,140,310,95,'Enroll and renew node',['Signed live node advertisement','NodeLeaseGuard + compatible registry','Start Directory + on-demand repos'],'amber')
d.box(740,140,300,95,'Serve with fencing',['Ready only while leases are valid','Per-Cell authority / actor / SQL','Cancel ingress when lease fails'],'green')
d.path([(350,187),(390,187)],color='amber')
d.path([(700,187),(740,187)],color='amber')
d.region(35,325,1030,455,'COLD ACTIVATION / NODE LOSS / LOCAL DISK LOSS','blue')
d.box(60,380,290,100,'Read catalog and control',['Validate compiled code and schema','Live owner: route to that owner','Idle or expired: acquire authority'],'blue')
d.box(390,380,310,100,'Fence and choose exact root',['Claim expired node for takeover','Use CellAuthority-pinned root','Do not elect a root by listing keys'],'orange')
d.box(740,380,300,100,'Verify and restore SQLite',['LTX chunk checksums / endpoints','Recover exact state + outcome ledger','Invalid bytes: fail without activation'],'violet')
d.box(390,590,310,105,'Activate successor',['Same CellTarget / repository UUID','New fenced owner / incarnation','Recovered refs, ACL and outcomes'],'blue')
d.box(740,590,300,105,'Rebuild Git cache',['Hydrate verified published objects','Recreate private ref snapshots','Resume Git/API/LFS requests'],'slate',True)
d.path([(350,430),(390,430)],color='blue')
d.path([(700,430),(740,430)],color='violet')
d.path([(890,480),(890,540),(545,540),(545,590)],color='violet')
d.path([(700,642),(740,642)],color='slate',dashed=True)
d.note(60,590,290,'Cache loss is recoverable','Cell publication protects acknowledged state. Files from an old workspace never become a substitute recovery root.', 'rose',105)
d.txt(60,749,'Recovery is on demand. An unclean owner loss waits for lease expiry; acquisition and restore stay supervised.',9)
d.region(35,830,1030,220,'DRAIN, MAINTENANCE AND INDEPENDENT BACKUP','amber')
d.box(60,880,290,110,'Graceful shutdown',['Stop public ingress; finish admitted work','Drain Cells + close native descendants','Withdraw advertisement / release disk','Workspace stays locked through cleanup'],'amber')
d.box(390,880,310,110,'Deployment maintenance',['Close release admission with operation ID','Fleet drains; prove all Cells settled','Recovery worker fences expired owners','Explicit end reopens same release'],'amber')
d.box(740,880,300,110,'Backup and restore',['Copy pinned roots + referenced bodies','Verify independent disjoint prefix','Restore into unused reserved prefix','Same provider; preserve identity'],'violet')
d.legend([('amber','node / deployment control'),('orange','owner fence'),('violet','verified recovery bytes'),('blue','same Cell / new owner')])
d.save('Canopy locks its local runtime workspace, verifies storage capabilities, validates the selected release and enrolls a signed node lease before serving. New or cold Cells bootstrap, acquire idle authority or fence an expired owner and restore the authority-pinned SQLite root. Recovery preserves the request outcome ledger. Native caches rebuild afterward. Shutdown and maintenance supervise all admitted work; backup copies durable roots and referenced external bodies into an independent prefix.', ['crates/canopy-server/src/server/mod.rs','crates/canopy-server/src/server/lifecycle.rs','crates/canopy-server/src/server/workspace/mod.rs','crates/canopy-server/src/deployment/recovery.rs','crates/canopy-server/src/deployment/backup/mod.rs','docs/operations.md'])

# 8. Domain composition and example merge control flow.
d=Diagram('08-domain-and-policy','Repository components and collaboration control','Git state, authorization and collaboration meet at one Repository Cell transaction boundary.',1130)
d.region(35,95,1030,350,'ONE REPOSITORY CELL / ONE UUID / ONE FENCED WRITER','violet')
for x,y,title,lines,c in [
    (60,150,'Git identity and history',['Object format, objects and graph','Refs, versions, HEAD, generation'],'violet'),
    (400,150,'Access and policy',['Owner / members / visibility','Branch rules and version guards'],'rose'),
    (740,150,'Issues and discussions',['Issues, comments and edits','Request records / pagination'],'green'),
    (60,295,'Pull requests and reviews',['Exact base/head comparisons','Reviews, threads and resolution'],'green'),
    (400,295,'Checks and candidates',['Commit-bound check attempts','Merge / squash / rebase candidates'],'green'),
    (740,295,'Durable operation outcomes',['Push reports / certificate audit','Mutation replay and receipts'],'blue')]:d.box(x,y,300,100,title,lines,c)
d.region(35,495,1030,335,'EXAMPLE: MERGE A PULL REQUEST','green')
d.box(60,550,300,105,'Prepare candidate',['Authenticate and capture base/head','Native merge-tree / commit-tree','Persist candidate objects + closure'])
d.box(400,550,300,105,'Publish with MergePull',['Recheck actor and exact revisions','Required checks / reviews / threads','Branch policy + ref expectations'],'rose')
d.box(740,550,300,105,'Commit and acknowledge',['Update branch ref + generation','Close PR + save merge outcome','Cellule gates durable result'],'blue')
d.path([(360,602),(400,602)],color='green')
d.path([(700,602),(740,602)],color='blue')
d.note(60,710,440,'Preparation is provisional','A candidate or passed check cannot grant publication. The final command checks the current branch and exact candidate/head bindings.', 'rose',85)
d.note(570,710,470,'CI and web boundaries','Checks are product records submitted through the API. Canopy does not install a Repository Cell workflow runner for CI today.', 'amber',85)
d.region(35,880,1030,175,'DIRECTORY AND REPOSITORY ARE SEPARATE TRANSACTION DOMAINS','orange')
d.box(60,930,440,85,'Directory Cell',['Global accounts / token scopes / SSH keys','Names and discovery candidates; pending -> ready'],'violet')
d.box(570,930,470,85,'Repository Cell',['Actual membership, visibility and write policy','Each sensitive transaction checks its own authority'],'violet')
d.path([(500,974),(570,974)],'UUID','orange')
d.legend([('violet','durable domain state'),('rose','final authorization / guards'),('green','collaboration logic'),('blue','durable outcome')])
d.save('The Repository Cell colocates Git refs and graph metadata with ACLs, visibility, issues, PRs, reviews, line threads, check attempts, branch rules and operation outcomes. Merge preparation may run native Git outside the authority boundary, but MergePull rechecks current exact revisions, policy and required evidence before atomically changing the branch and PR state. Directory and Repository state remain separate Cells.', ['crates/canopy-server/src/schema.sql','crates/canopy-server/src/pulls/merge/command.rs','crates/canopy-server/src/pulls/candidates/mod.rs','crates/canopy-server/src/branch_rules/command.rs','crates/canopy-server/src/server/discovery.rs'])

# 9. Implemented primitives versus production cutover and capability proposal.
d=Diagram('09-packed-storage-evolution','Packed storage evolution and remaining integration','These primitives exist in code; the replacement production request path and fresh-schema cutover remain open.',1220,status='INCOMPLETE PRODUCT INTEGRATION')
d.region(35,95,1030,185,'ACTIVE SERVING MODEL','green')
d.box(60,140,440,95,'Per-object SQL authority',['Canonical objects + edges + closure in Repository Cell','Inline / chunks / external / archived packed blobs','CompletePush publishes refs and stored response'])
d.box(570,140,470,95,'Current gateway preparation',['Serialized native push phase within each gateway','Verified immutable body / archive uploads','SQL ingestion batches and graph certification'])
d.region(35,330,1030,475,'NEW PACKED CATALOG MODEL / IMPLEMENTED BUILDING BLOCKS','blue')
d.box(60,380,290,110,'Private native preparation',['Retain exact input + native result','Owned staging / preparation attempts','Pack + index + metadata artifacts','Verify canonical identity and closure'],'slate')
d.box(390,380,310,110,'Immutable catalog metadata',['Sorted directory runs + leveled index','Source roots identify physical packs','Catalog binds directory + sources','Immutable ref-state / outcome roots'],'violet')
d.box(740,380,300,110,'Publication coordinator',['Bounded class / account dispatch','Register exact SDK command identity','Bind owner fence / attempt / base','Trusted certificates + ref policy pages'],'orange')
d.box(390,590,310,115,'Short Cell publication',['Check current ACL / policy / generation','Publish catalog + ref root + outcome','Retain generation / recovery facts','Cellule still supplies durability gate'],'blue')
d.box(740,590,300,115,'Pinned catalog readers',['Verify catalog / source descriptors','Resolve OID -> pack location','Independent bounded reader facilities','Do not infer access from pack membership'],'violet')
d.path([(350,435),(390,435)],color='violet')
d.path([(700,435),(740,435)],color='orange')
d.path([(890,490),(890,540),(545,540),(545,590)],color='orange')
d.path([(700,648),(740,648)],'certified root','violet')
d.txt(60,758,'packs/publication/mod.rs explicitly says these commands are not registered on the legacy repository serving path.',9,'#fbbf24')
d.note(35,855,490,'Required production work','Wire startup, HTTP, SSH and generated producers/readers; select the fresh schema; complete recovery, collection, backup and capacity qualification.', 'amber',130)
d.note(575,855,490,'Separate Cell capability proposal','Adding KV, Queue, Workflow, Blob, Cron, Timer and Effects to the same Repository Cell needs framework composition and autonomous-progress acceptance gates.', 'amber',130)
d.note(35,1035,1030,'What does not change in the intended design','The repository UUID, one fenced authoritative publisher, final policy checks and durable outcome acknowledgement remain. Immutable uploaded artifacts are preparation until an authorized Cell command publishes them.', 'blue',90)
d.legend([('green','active serving path'),('blue','new implemented primitives'),('violet','immutable metadata / bytes'),('amber','integration and qualification open')])
d.save('The current product still uses per-object SQL authority, with an active archived-pack optimization. The new subsystem moves canonical inventories, graph metadata, ref snapshots and exact outcomes into verified immutable artifacts and short Cell publication commands. Preparation, publication dispatch, certificates, policy pages, recovery records and readers exist, but the full serving path is not registered and the hard cutover remains incomplete. Multi-capability repository Cells are a separate proposal.', ['crates/canopy-server/src/packs/publication/mod.rs','crates/canopy-server/src/packs/publication/coordinator.rs','crates/canopy-server/src/packs/catalog/mod.rs','crates/canopy-server/src/packs/directory/mod.rs','crates/canopy-server/src/packs/ref_state/mod.rs','docs/large-repository-implementation-status.md','docs/large-team-scalability.md','docs/repository-cell-primitives.md'])

(OUT/'manifest.json').write_text(json.dumps(ATLAS,indent=2)+'\n')
print(f'Generated {len(ATLAS)} SVG diagrams in {OUT}')

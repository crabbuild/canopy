#!/usr/bin/env python3
"""Execute the proposed DDL with Canopy's unchanged product tables.

Design validation only: no Canopy processes, deployments or persistent databases.
Run from any directory with Python 3 (stdlib only).
"""
from pathlib import Path
import re
import sqlite3
import unittest

ROOT = Path(__file__).resolve().parents[2]
REPLACED = {
    "object_uploads", "object_chunks", "objects", "object_edges", "object_closure",
    "object_pending", "refs", "ref_generation",
}


def schema():
    # Split using SQLite's parser so comments and quoted semicolons are safe.
    statements, pending = [], ""
    for line in (ROOT / "crates/canopy-server/src/schema.sql").read_text().splitlines(keepends=True):
        pending += line
        if sqlite3.complete_statement(pending):
            clean = re.sub(r"--[^\n]*", "", pending).strip()
            table = re.search(r"^CREATE TABLE (\w+)", clean)
            index = re.search(r"^CREATE (?:UNIQUE )?INDEX \w+ ON (\w+)", clean)
            insert = re.search(r"^INSERT INTO (\w+)", clean)
            target = table or index or insert
            if target is None or target.group(1) not in REPLACED:
                statements.append(pending)
            pending = ""
    if pending.strip():
        raise AssertionError("Unparsed source SQL")
    return (ROOT / "docs/design/packed-repository-schema.sql").read_text() + "\n" + "".join(statements)


class PackedSchema(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.executescript(schema())
        self.db.execute("INSERT INTO repository_identity VALUES ('sha1',1,?,'owner',?)", (b'r'*16, b's'*32))
        self.db.execute("INSERT INTO pack_operations VALUES (?,'ingest',?,1,1,'open',0,0)", (b'o'*16,b'i'*16))

    def tearDown(self):
        self.db.close()

    def pack(self, byte=1, sealed=False, fmt='sha1'):
        width = 20 if fmt == 'sha1' else 32
        digest = bytes([byte])*32
        cur = self.db.execute("""INSERT INTO packs(operation_id,digest,object_format,git_checksum,
            size,manifest_digest,index_size,index_digest,index_manifest_digest,object_count,
            inventory_digest,staged_digest) VALUES (?,?,?,?,1,?,1,?,?,1,?,?)""",
            (b'o'*16,digest,fmt,b'c'*width,digest,digest,digest,digest,digest))
        pk = cur.lastrowid
        if sealed:
            self.db.execute("UPDATE packs SET staged_count=1,last_oid=?,state='sealed',sealed_generation=id WHERE id=?",(b'x'*width,pk))
        return pk

    def seal(self, pack):
        self.db.execute("UPDATE packs SET staged_count=object_count,last_oid=?,state='sealed',sealed_generation=id WHERE id=?",(b'x'*20,pack))

    def obj(self, oid, pack, kind='blob', edges=0):
        self.db.execute("""INSERT INTO objects(oid,kind,size,digest,pack_id,edge_count,edge_digest)
            VALUES (?,?,0,?,?,?,?)""",(oid,kind,b'd'*32,pack,edges,b'e'*32))
        seq = self.db.execute("SELECT sequence FROM objects WHERE oid=?",(oid,)).fetchone()[0]
        self.db.execute("INSERT INTO object_pending(sequence,oid,edge_digest) VALUES (?,?,?)",(seq,oid,b'e'*32))

    def test_composes_with_collaboration_schema(self):
        self.assertEqual(self.db.execute("PRAGMA foreign_key_check").fetchall(), [])
        tables = {r[0] for r in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertTrue({'pull_requests','pushes','commit_parents','lfs_objects'} <= tables)
        self.assertFalse({'object_uploads','object_chunks','object_locations','git_pack_parts'} & tables)

    def test_format_and_incomplete_seal(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self.pack(fmt='sha256')
        p = self.pack()
        with self.assertRaises(sqlite3.IntegrityError):
            self.obj(b'x'*32,p)
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute("UPDATE packs SET state='sealed' WHERE id=?",(p,))

    def test_sha256(self):
        self.db.execute("UPDATE repository_identity SET object_format='sha256'")
        self.obj(b'x'*32,self.pack(fmt='sha256'))
        self.assertEqual(self.db.execute("PRAGMA foreign_key_check").fetchall(), [])

    def test_identity_location_and_retirement(self):
        old, new = self.pack(), self.pack(2,sealed=True)
        self.obj(b'x'*20,old)
        self.seal(old)
        with self.assertRaises(sqlite3.IntegrityError):
            self.obj(b'x'*20,new)
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute("UPDATE objects SET digest=?",(b'z'*32,))
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute("UPDATE objects SET pack_id=?",(new,))
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute("UPDATE packs SET state='retired' WHERE id=?",(old,))
        self.db.execute("UPDATE objects SET pack_id=?,location_version=2 WHERE pack_id=? AND location_version=1",(new,old))
        self.db.execute("UPDATE packs SET state='retired' WHERE id=?",(old,))
        stale = self.db.execute("UPDATE objects SET pack_id=?,location_version=3 WHERE pack_id=? AND location_version=1",(new,old))
        self.assertEqual(stale.rowcount,0)
        self.assertEqual(self.db.execute("SELECT sequence, location_version FROM objects").fetchone(),(1,2))

    def test_reverse_edge_propagation_and_late_parent(self):
        p=self.pack()
        leaf, parent, late = b'l'*20,b'p'*20,b'q'*20
        self.obj(leaf,p)
        for oid in (parent,late):
            self.obj(oid,p,'tree',1)
        self.seal(p)
        self.db.execute("INSERT INTO object_edges VALUES (?,?,'blob',1)",(parent,leaf))
        self.db.execute("UPDATE object_pending SET received_edges=1,last_child=?,remaining_children=1,edges_complete=1 WHERE oid=?",(leaf,parent))
        self.db.execute("INSERT INTO object_closure VALUES (?)",(leaf,))
        for _ in range(2):
            changed=self.db.execute("UPDATE object_edges SET waiting=0 WHERE parent=? AND child=? AND waiting=1",(parent,leaf)).rowcount
            self.db.execute("UPDATE object_pending SET remaining_children=remaining_children-? WHERE oid=?",(changed,parent))
        self.assertEqual(self.db.execute("SELECT remaining_children FROM object_pending WHERE oid=?",(parent,)).fetchone(),(0,))
        self.db.execute("INSERT INTO object_edges SELECT ?,?,'blob',NOT EXISTS(SELECT 1 FROM object_closure WHERE oid=?)",(late,leaf,leaf))
        self.assertEqual(self.db.execute("SELECT waiting FROM object_edges WHERE parent=?",(late,)).fetchone(),(0,))

    def test_deleted_artifact_can_be_reimported_with_new_id(self):
        old=self.pack(sealed=True)
        with self.assertRaises(sqlite3.IntegrityError):
            self.pack()
        self.db.execute("UPDATE packs SET state='retired' WHERE id=?",(old,))
        self.db.execute("UPDATE packs SET state='deleting' WHERE id=?",(old,))
        self.db.execute("UPDATE packs SET state='deleted' WHERE id=?",(old,))
        new=self.pack()
        self.assertGreater(new,old)

    def test_missing_dependencies_rejected(self):
        p=self.pack()
        self.obj(b'p'*20,p,'tree',1)
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute("INSERT INTO object_edges VALUES (?,?,'blob',1)",(b'p'*20,b'x'*20))


if __name__ == '__main__':
    unittest.main(verbosity=2)

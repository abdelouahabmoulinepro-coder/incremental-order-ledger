from concurrent.futures import ThreadPoolExecutor
import copy
import itertools
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from ledger import ingest,snapshot,validate
from demo import run_demo


def event(eid='A1',oid='A',version=1,total=100):
    return {'event_id':eid,'order_id':oid,'version':version,'operation':'upsert','status':'confirmed','total_cents':total}


def batch(bid='b1',events=None):
    return {'batch_id':bid,'events':[event()] if events is None else events}


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db=Path(self.tmp.name)/'ledger.sqlite'

    def test_same_batch_replay_is_noop(self):
        initial=ingest(self.db,batch())
        before=snapshot(self.db)
        again=ingest(self.db,batch())
        self.assertTrue(again['replayed'])
        self.assertEqual(initial|{'replayed':True},again)
        self.assertEqual(before,snapshot(self.db))

    def test_batch_id_content_conflict(self):
        ingest(self.db,batch())
        with self.assertRaisesRegex(ValueError,'Batch id'):
            ingest(self.db,batch(events=[event(total=999)]))
        self.assertEqual(snapshot(self.db)['confirmed_total_cents'],100)

    def test_duplicate_event_within_and_between_batches(self):
        stats=ingest(self.db,batch(events=[event(),event()]))
        self.assertEqual((stats['new_events'],stats['duplicates']),(1,1))
        stats=ingest(self.db,batch('b2'))
        self.assertEqual((stats['new_events'],stats['duplicates']),(0,1))
        self.assertEqual(snapshot(self.db)['event_count'],1)

    def test_conflicting_event_rolls_back_prior_write(self):
        ingest(self.db,batch())
        before=snapshot(self.db)
        with self.assertRaisesRegex(ValueError,'Event id'):
            ingest(self.db,batch('bad',[event('B1','B'),event(total=999)]))
        self.assertEqual(before,snapshot(self.db))

    def test_historical_version_conflict_detected(self):
        ingest(self.db,batch(events=[event(),event('A3',version=3)]))
        with self.assertRaisesRegex(ValueError,'version'):
            ingest(self.db,batch('bad',[event('other',version=1)]))
        self.assertEqual(snapshot(self.db)['event_count'],2)

    def test_late_unknown_version_kept_but_not_applied(self):
        ingest(self.db,batch(events=[event('A3',version=3,total=300)]))
        stats=ingest(self.db,batch('late',[event()]))
        self.assertEqual((stats['new_events'],stats['stale'],stats['applied']),(1,1,0))
        self.assertEqual(snapshot(self.db)['confirmed_total_cents'],300)

    def test_tombstone_blocks_old_event_allows_new_version(self):
        tombstone={'event_id':'A3','order_id':'A','version':3,'operation':'delete'}
        ingest(self.db,batch(events=[tombstone]))
        ingest(self.db,batch('old',[event()]))
        self.assertEqual(snapshot(self.db)['active_orders'],0)
        ingest(self.db,batch('restore',[event('A4',version=4,total=400)]))
        self.assertEqual(snapshot(self.db)['confirmed_total_cents'],400)

    def test_all_24_arrival_orders_have_same_snapshot(self):
        events=[event(),event('A2',version=2,total=200),
                {'event_id':'A3','order_id':'A','version':3,'operation':'delete'},event('B1','B',total=700)]
        reference=None
        for i,permutation in enumerate(itertools.permutations(events)):
            path=Path(self.tmp.name)/f'case{i}.sqlite'
            ingest(path,batch(events=list(permutation)))
            state=snapshot(path)
            if reference is None: reference=state
            self.assertEqual(reference,state)
        self.assertEqual(reference['confirmed_total_cents'],700)

    def test_concurrent_same_batch_is_serialized(self):
        ingest(self.db,batch('empty',[]))
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(lambda _:ingest(self.db,batch()),range(2)))
        self.assertEqual(sorted(r['replayed'] for r in results),[False,True])
        self.assertEqual(snapshot(self.db)['event_count'],1)

    def test_cancelled_order_excluded_from_confirmed_total(self):
        ingest(self.db,batch(events=[event()|{'status':'cancelled'}]))
        state=snapshot(self.db)
        self.assertEqual((state['active_orders'],state['confirmed_total_cents']),(1,0))

    def test_invalid_batch_rejected_before_database_creation(self):
        for changes in [{'version':True},{'total_cents':-1},{'operation':'other'},{'extra':'value'}]:
            with self.assertRaises(ValueError): ingest(self.db,batch(events=[event()|changes]))
        self.assertFalse(self.db.exists())

    def test_snapshot_missing_database_does_not_create(self):
        with self.assertRaises(ValueError): snapshot(self.db)
        self.assertFalse(self.db.exists())

    def test_canonical_json_key_order(self):
        ingest(self.db,batch())
        changed={'events':[dict(reversed(list(event().items())))],'batch_id':'b1'}
        self.assertTrue(ingest(self.db,changed)['replayed'])

    def test_demo_counts_and_full_rollback(self):
        report=run_demo()
        self.assertTrue(report['rollback_verified'])
        self.assertEqual(report['final']['confirmed_total_cents'],21500)
        self.assertEqual(report['final']['event_count'],9)
        self.assertEqual(report['final']['batch_count'],3)

    def test_cli_ingest_and_snapshot(self):
        proc=subprocess.run([sys.executable,'ledger.py','--db',str(self.db),'ingest','data/01_initial.json'],capture_output=True,text=True)
        self.assertEqual(proc.returncode,0,proc.stderr)
        proc=subprocess.run([sys.executable,'ledger.py','--db',str(self.db),'snapshot'],capture_output=True,text=True)
        self.assertEqual(proc.returncode,0,proc.stderr)
        self.assertEqual(json.loads(proc.stdout)['confirmed_total_cents'],30000)


if __name__=='__main__':
    unittest.main()

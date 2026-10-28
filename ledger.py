"""Transactional, versioned order ingestion with retries and deletion tombstones."""
import argparse
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import re
import sqlite3


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=True)


def identifier(value):
    return isinstance(value,str) and bool(re.fullmatch(r'[A-Za-z0-9_-]{1,64}',value))


def validate(batch):
    if not isinstance(batch,dict) or set(batch)!={'batch_id','events'} or not identifier(batch['batch_id']):
        raise ValueError('Batch requires only batch_id and events; id must be short ASCII')
    if not isinstance(batch['events'],list) or len(batch['events'])>10000:
        raise ValueError('events must be a list of at most 10000 records')
    for event in batch['events']:
        if not isinstance(event,dict):
            raise ValueError('Each event must be an object')
        required={'event_id','order_id','version','operation'}
        if event.get('operation')=='upsert':
            required|={'status','total_cents'}
        elif event.get('operation')!='delete':
            raise ValueError('Unknown operation')
        if set(event)!=required:
            raise ValueError('Missing or unexpected event fields')
        if not identifier(event['event_id']) or not identifier(event['order_id']):
            raise ValueError('Invalid event/order identifier')
        if type(event['version']) is not int or not 1<=event['version']<=2**31-1:
            raise ValueError('version must be a positive 32-bit integer')
        if event['operation']=='upsert':
            if event['status'] not in ('confirmed','cancelled'):
                raise ValueError('Unknown status')
            if type(event['total_cents']) is not int or not 0<=event['total_cents']<=10**12:
                raise ValueError('total_cents must be an integer between 0 and 1000000000000')


@contextmanager
def connection(path,initialize=True):
    path=Path(path)
    if initialize:
        path.parent.mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(path if initialize else path.resolve().as_uri()+'?mode=ro',
                       uri=not initialize,timeout=5,isolation_level=None)
    db.row_factory=sqlite3.Row
    try:
        if initialize:
            db.executescript(Path(__file__).with_name('schema.sql').read_text(encoding='utf-8'))
        yield db
    finally:
        db.close()


def ingest(path,batch):
    validate(batch)
    digest=hashlib.sha256(canonical(batch).encode()).hexdigest()
    with connection(path) as db:
        db.execute('BEGIN IMMEDIATE')
        try:
            existing=db.execute('SELECT digest,result FROM batches WHERE batch_id=?',(batch['batch_id'],)).fetchone()
            if existing:
                if existing['digest']!=digest:
                    raise ValueError('Batch id reused with different content')
                db.execute('COMMIT')
                return json.loads(existing['result'])|{'replayed':True}
            stats={'batch_id':batch['batch_id'],'new_events':0,'duplicates':0,'applied':0,'stale':0,'replayed':False}
            for event in batch['events']:
                payload=canonical(event)
                existing=db.execute('SELECT payload FROM events WHERE event_id=?',(event['event_id'],)).fetchone()
                if existing:
                    if existing['payload']!=payload:
                        raise ValueError('Event id reused with different content')
                    stats['duplicates']+=1
                    continue
                if db.execute('SELECT 1 FROM events WHERE order_id=? AND version=?',(event['order_id'],event['version'])).fetchone():
                    raise ValueError('Order version already claimed by another event id')
                db.execute('INSERT INTO events VALUES (?,?,?,?)',(event['event_id'],event['order_id'],event['version'],payload))
                stats['new_events']+=1
                current=db.execute('SELECT version FROM orders WHERE order_id=?',(event['order_id'],)).fetchone()
                if current and current['version']>=event['version']:
                    stats['stale']+=1
                    continue
                deleted=int(event['operation']=='delete')
                db.execute('''INSERT INTO orders VALUES (?,?,?,?,?)
                    ON CONFLICT(order_id) DO UPDATE SET version=excluded.version,deleted=excluded.deleted,
                    status=excluded.status,total_cents=excluded.total_cents''',
                    (event['order_id'],event['version'],deleted,event.get('status'),event.get('total_cents',0)))
                stats['applied']+=1
            db.execute('INSERT INTO batches VALUES (?,?,?)',(batch['batch_id'],digest,canonical(stats)))
            db.execute('COMMIT')
            return stats
        except BaseException:
            if db.in_transaction:
                db.execute('ROLLBACK')
            raise


def snapshot(path):
    if not Path(path).is_file():
        raise ValueError('Database does not exist; ingest a batch first')
    with connection(path,initialize=False) as db:
        db.execute('BEGIN')
        try:
            orders=[dict(row) for row in db.execute('SELECT * FROM orders ORDER BY order_id')]
            counts={name:db.execute(f'SELECT count(*) FROM {name}').fetchone()[0] for name in ('events','batches')}
            db.execute('COMMIT')
        except BaseException:
            db.execute('ROLLBACK')
            raise
    # Python integer sum avoids SQLite 64-bit aggregate overflow on long-lived ledgers.
    confirmed=[row for row in orders if not row['deleted'] and row['status']=='confirmed']
    return {'orders':orders,'event_count':counts['events'],'batch_count':counts['batches'],
            'active_orders':sum(not row['deleted'] for row in orders),
            'deleted_orders':sum(row['deleted'] for row in orders),
            'confirmed_orders':len(confirmed),'confirmed_total_cents':sum(row['total_cents'] for row in confirmed)}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db',default='build/ledger.sqlite')
    commands=parser.add_subparsers(dest='command',required=True)
    ingest_parser=commands.add_parser('ingest')
    ingest_parser.add_argument('batch')
    commands.add_parser('snapshot')
    args=parser.parse_args()
    try:
        if args.command=='ingest':
            if Path(args.db).resolve()==Path(args.batch).resolve():
                raise ValueError('Database and input paths must differ')
            result=ingest(args.db,json.loads(Path(args.batch).read_text(encoding='utf-8-sig')))
        else:
            result=snapshot(args.db)
        print(json.dumps(result,indent=2))
    except (ValueError,OSError,sqlite3.Error) as error:
        parser.exit(1,f'Ingestion error: {error}\n')


if __name__=='__main__':
    main()

"""Run the reproducible scenarios in a temporary database; preserve only the report."""
import argparse
import json
from pathlib import Path
import tempfile
from ledger import ingest,snapshot


def run_demo():
    root=Path(__file__).parent/'data'
    load=lambda name:json.loads((root/name).read_text(encoding='utf-8'))
    with tempfile.TemporaryDirectory() as directory:
        db=Path(directory)/'demo.sqlite'
        stages=[]
        for name in ['01_initial.json','02_changes.json','02_changes.json','03_late_event.json']:
            stats=ingest(db,load(name))
            state=snapshot(db)
            stages.append({'input':name,'ingestion':stats,'snapshot':state})
        before=snapshot(db)
        try:
            ingest(db,load('04_conflict.json'))
        except ValueError as error:
            failure=str(error)
        else:
            raise AssertionError('Conflict should have rejected the batch')
        after=snapshot(db)
        if before!=after:
            raise AssertionError('Failed batch changed the ledger')
        return {'stages':stages,'conflict_error':failure,'rollback_verified':True,'final':after}


def markdown(report):
    lines=['# Incremental Order Ledger', '', '> Commandes fictives. Montants en centimes, état métier courant, aucune comptabilité de flux.', '',
           '| Étape | Lot rejoué | Nouveaux événements dans ce lot | Actifs | Supprimés | Total confirmé |', '|---|---|---:|---:|---:|---:|']
    for stage in report['stages']:
        state,stats=stage['snapshot'],stage['ingestion']
        new='0 (rejeu)' if stats['replayed'] else str(stats['new_events'])
        lines.append(f"| {stage['input']} | {stats['replayed']} | {new} | {state['active_orders']} | {state['deleted_orders']} | {state['confirmed_total_cents']/100:.2f} EUR |")
    lines+=['', 'Le rejeu renvoie les statistiques historiques du lot avec `replayed=true` ; il n’écrit aucun nouvel événement.', '',
            f"Conflit volontaire : `{report['conflict_error']}`.",
            f"Annulation intégrale vérifiée : **{report['rollback_verified']}**. La commande E du lot rejeté n’a pas été conservée.", '',
            f"État final : {report['final']['event_count']} événements uniques, {report['final']['batch_count']} lots validés, "
            f"{report['final']['confirmed_orders']} commandes confirmées pour {report['final']['confirmed_total_cents']/100:.2f} EUR.", '']
    return '\n'.join(lines)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',default='build/demo')
    args=parser.parse_args()
    report=run_demo()
    output=Path(args.output)
    output.mkdir(parents=True,exist_ok=True)
    (output/'results.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    (output/'report.md').write_text(markdown(report),encoding='utf-8')
    print(json.dumps(report['final'],indent=2))

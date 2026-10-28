# Incremental Order Ledger

> Commandes fictives. Montants en centimes, état métier courant, aucune comptabilité de flux.

| Étape | Lot rejoué | Nouveaux événements dans ce lot | Actifs | Supprimés | Total confirmé |
|---|---|---:|---:|---:|---:|
| 01_initial.json | False | 3 | 3 | 0 | 300.00 EUR |
| 02_changes.json | False | 5 | 3 | 1 | 215.00 EUR |
| 02_changes.json | True | 0 (rejeu) | 3 | 1 | 215.00 EUR |
| 03_late_event.json | False | 1 | 3 | 1 | 215.00 EUR |

Le rejeu renvoie les statistiques historiques du lot avec `replayed=true` ; il n’écrit aucun nouvel événement.

Conflit volontaire : `Order version already claimed by another event id`.
Annulation intégrale vérifiée : **True**. La commande E du lot rejeté n’a pas été conservée.

État final : 9 événements uniques, 3 lots validés, 3 commandes confirmées pour 215.00 EUR.

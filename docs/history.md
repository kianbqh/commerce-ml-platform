# Imported history

The existing warehouse repository was renamed in place to `commerce-ml-platform`. Its original main is `7e645263872e3de426338add0b69924db1dd321c`. Moving its files beneath `warehouse/` produced commit `589837d`.

The recommendation source was added under `recsys/` without squash in merge `08838d6`, preserving main `e06f613e1b1c81adf2a02a023777e1aaca1acf75`. The experiment source was added under `experiments/` without squash in merge `c431efd`, preserving main `6645eb94083a2531caa33a2d97473605b769c46b`. All three original mains remain ancestors, with their existing authors and timestamps.

The integration then replaced active MovieLens loading/metrics/entities with Instacart next-basket recommendations. Prior recommendation and warehouse output snapshots are in `results/before/`. Hillstrom remains its own randomized experiment dataset, with its notebook and reports under `experiments/`.

# MLB Exhaustive Coupled Run-Model Search

- Generated: `2026-09-12T02:48:00.323800+00:00`
- Production artifacts modified: `NO`
- Final untouched test used for model selection: `NO`
- Feature sets: `8`
- Model families: `7`
- Successful home candidates: `1588`
- Successful away candidates: `1472`
- Home shortlist: `452`
- Away shortlist: `437`
- Coupled pairs evaluated: `197524`

## Model-family grid sizes per feature set

- `hist_gradient_boosting`: `48` configurations
- `random_forest`: `16` configurations
- `extra_trees`: `16` configurations
- `poisson_glm`: `5` configurations
- `tweedie_glm`: `9` configurations
- `lightgbm`: `36` configurations
- `xgboost`: `36` configurations

## Walk-forward CV folds

- Fold 1: train `2026-03-26` through `2026-06-02`; validate `2026-06-03` through `2026-06-20`
- Fold 2: train `2026-03-26` through `2026-06-20`; validate `2026-06-21` through `2026-07-07`
- Fold 3: train `2026-03-26` through `2026-07-07`; validate `2026-07-08` through `2026-07-27`
- Fold 4: train `2026-03-26` through `2026-07-27`; validate `2026-07-28` through `2026-08-13`

## Validation winner

- Home: `random_forest` / `no_dratings_prob` / `671c8f6c42c7bf84430c`
- Away: `poisson_glm` / `dratings_plus_pitcher` / `15fad4fc350fd375711d`
- Composite ratio vs DRatings: `0.99019131`
- Worst component ratio vs DRatings: `1.00738060`

## Untouched final test

- Composite ratio vs DRatings: `0.98510869`
- Worst component ratio vs DRatings: `1.01623015`

| Metric | Winner | DRatings | Ratio |
| --- | ---: | ---: | ---: |
| home_poisson | 2.30942059 | 2.27253697 | 1.01623015 |
| away_poisson | 2.36188316 | 2.46010541 | 0.96007397 |
| moneyline_log_loss | 0.65888020 | 0.66439423 | 0.99170067 |
| runline_minus_1_5_log_loss | 0.64877281 | 0.65280085 | 0.99382961 |
| runline_plus_1_5_log_loss | 0.65504638 | 0.68025687 | 0.96293976 |
| totals_halfline_log_loss | 0.66575687 | 0.68131898 | 0.97715884 |
| margin_nll | 3.16584428 | 3.21759555 | 0.98391617 |
| total_nll | 3.07596875 | 3.08759547 | 0.99623438 |

## Interpretation

- Ratio `< 1.0`: winner beat DRatings on that metric.
- Ratio `> 1.0`: DRatings was better on that metric.
- The composite is the geometric mean of the eight ratios.
- Winner files remain research artifacts until production integration is explicitly approved.

# MLB Run Model Comparison

- Generated: `2026-08-30T12:56:38.105781+00:00`
- Untouched chronological test period: `2026-08-02` through `2026-08-28`
- Test games: `138`
- Model fitting/tuning performed by this evaluation script: `NO`

## Run prediction metrics

| System | Side | Rows | MAE | Mean Poisson deviance | Mean predicted runs | Mean actual runs |
| --- | --- | --- | --- | --- | --- | --- |
| dratings | home | 138 | 2.344783 | 2.296256 | 4.365942 | 4.420290 |
| new_model | home | 138 | 2.376807 | 2.320936 | 4.330125 | 4.420290 |
| dratings | away | 138 | 2.460290 | 2.438597 | 4.167391 | 3.833333 |
| new_model | away | 138 | 2.559863 | 2.572760 | 4.418253 | 3.833333 |

### Run-prediction questions

- Does the new model improve home-run prediction error? **NO** (MAE `2.344783` -> `2.376807`; Poisson deviance `2.296256` -> `2.320936`).
- Does the new model improve away-run prediction error? **NO** (MAE `2.460290` -> `2.559863`; Poisson deviance `2.438597` -> `2.572760`).

## Probability calibration

Calibration YES/NO uses weighted expected calibration error (ECE) <= `0.05`. Totals use conditional win probability on resolved bets; pushes are excluded from the observed win-rate denominator.

| Market | New-model ECE | Calibrated | Predicted-vs-observed Spearman | Observed rate exactly non-decreasing | Populated bins |
| --- | --- | --- | --- | --- | --- |
| moneyline | 0.142035 | NO | -0.285714 | NO | 8 |
| run_line | 0.152059 | NO | 0.946125 | NO | 8 |
| total | 0.148064 | NO | -0.476190 | NO | 8 |

- Are predicted moneyline probabilities calibrated? **NO**.
- Are predicted run-line probabilities calibrated? **NO**.
- Are predicted total probabilities calibrated? **NO**.
- Does increasing predicted probability correspond to increasing observed win rate? Moneyline **NO**, run line **NO**, total **NO**. See Spearman values above for rank-direction strength.

## Probability log loss

| System | Market | Evaluation side | Rows | Log loss |
| --- | --- | --- | --- | --- |
| dratings | moneyline | home | 138 | 0.692492 |
| dratings | run_line | home | 138 | 0.687247 |
| dratings | total | over_resolved | 136 | 0.728515 |
| new_model | moneyline | home | 138 | 0.721762 |
| new_model | run_line | home | 138 | 0.703269 |
| new_model | total | over_resolved | 136 | 0.733254 |

## EV, realized return, and Kelly

- New-model priced candidates evaluated: `828`; positive-EV candidates: `358`.
- New-model all-candidate mean predicted EV vs realized return: `-0.046539` vs `-0.043998`.
- New-model positive-EV mean predicted EV vs realized return: `0.166636` vs `-0.092486`.
- Does higher predicted EV correspond to higher realized return? EV/return Spearman = `-0.035939`. A positive value indicates higher EV tended to correspond to higher realized return in this test sample.
- Is positive EV overstated versus realized return? **YES** (defined here as mean realized return below mean predicted EV among positive-EV candidates).
- DRatings-run baseline all-candidate mean predicted EV vs realized return: `-0.050175` vs `-0.043998`; EV/return Spearman `-0.140437`.
- Does Kelly increase monotonically with actual model edge? Edge/Kelly-raw Spearman = `0.994724`; mean raw Kelly across ordered edge bins is non-decreasing: **YES** across `10` populated edge bins.

## Run-line side preference

- Games with both run-line sides priced/evaluated: `138`.
- Higher-EV side was `-1.5` in `48` games (`34.78%` of non-ties).
- Higher-EV side was `+1.5` in `90` games (`65.22%` of non-ties).
- Exact EV ties: `0`.

## Interpretation constraint

This report evaluates the saved model on the untouched test period only. The script does not refit, retune, or select hyperparameters from these results. Do not tune the model on this final test period after reviewing the report.

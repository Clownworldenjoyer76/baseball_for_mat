# MLB Run Model Comparison

- Generated: `2026-09-29T10:39:49.760779+00:00`
- Untouched chronological test period: `2026-09-01` through `2026-09-27`
- Test games: `85`
- Model fitting/tuning performed by this evaluation script: `NO`
- Promotion status: `candidate_rejected`

## Production promotion gate

Candidate promotion requires mean Poisson deviance <= the DRatings baseline for BOTH home and away models.

| Side | DRatings baseline Poisson | Candidate Poisson | Candidate <= baseline |
| --- | --- | --- | --- |
| home | 1.749894 | 1.950306 | NO |
| away | 2.209762 | 2.353040 | NO |

- Production artifacts changed: **NO**.

## Run prediction metrics

| System | Side | Rows | MAE | Mean Poisson deviance | Mean predicted runs | Mean actual runs |
| --- | --- | --- | --- | --- | --- | --- |
| dratings | home | 85 | 2.144824 | 1.749894 | 4.325529 | 4.352941 |
| new_model | home | 85 | 2.239082 | 1.950306 | 4.460674 | 4.352941 |
| dratings | away | 85 | 2.483882 | 2.209762 | 4.103176 | 5.270588 |
| new_model | away | 85 | 2.564853 | 2.353040 | 3.839511 | 5.270588 |

### Run-prediction questions

- Does the new model improve home-run prediction error? **NO** (MAE `2.144824` -> `2.239082`; Poisson deviance `1.749894` -> `1.950306`).
- Does the new model improve away-run prediction error? **NO** (MAE `2.483882` -> `2.564853`; Poisson deviance `2.209762` -> `2.353040`).

## Probability calibration

Calibration YES/NO uses weighted expected calibration error (ECE) <= `0.05`. Totals use conditional win probability on resolved bets; pushes are excluded from the observed win-rate denominator.

| Market | New-model ECE | Calibrated | Predicted-vs-observed Spearman | Observed rate exactly non-decreasing | Populated bins |
| --- | --- | --- | --- | --- | --- |
| moneyline | 0.170593 | NO | 0.680854 | NO | 10 |
| run_line | 0.250217 | NO | 0.151515 | NO | 10 |
| total | 0.243755 | NO | -0.380952 | NO | 8 |

- Are predicted moneyline probabilities calibrated? **NO**.
- Are predicted run-line probabilities calibrated? **NO**.
- Are predicted total probabilities calibrated? **NO**.
- Does increasing predicted probability correspond to increasing observed win rate? Moneyline **NO**, run line **NO**, total **NO**. See Spearman values above for rank-direction strength.

## Probability log loss

| System | Market | Evaluation side | Rows | Log loss |
| --- | --- | --- | --- | --- |
| dratings | moneyline | home | 85 | 0.653990 |
| dratings | run_line | home | 83 | 0.694497 |
| dratings | total | over_resolved | 82 | 0.709560 |
| new_model | moneyline | home | 85 | 0.759283 |
| new_model | run_line | home | 83 | 0.794176 |
| new_model | total | over_resolved | 82 | 0.737226 |

## EV, realized return, and Kelly

- New-model priced candidates evaluated: `506`; positive-EV candidates: `229`.
- New-model all-candidate mean predicted EV vs realized return: `-0.045735` vs `-0.055711`.
- New-model positive-EV mean predicted EV vs realized return: `0.229808` vs `-0.172489`.
- Does higher predicted EV correspond to higher realized return? EV/return Spearman = `-0.133400`. A positive value indicates higher EV tended to correspond to higher realized return in this test sample.
- Is positive EV overstated versus realized return? **YES** (defined here as mean realized return below mean predicted EV among positive-EV candidates).
- DRatings-run baseline all-candidate mean predicted EV vs realized return: `-0.056891` vs `-0.055711`; EV/return Spearman `-0.117051`.
- Does Kelly increase monotonically with actual model edge? Edge/Kelly-raw Spearman = `0.992413`; mean raw Kelly across ordered edge bins is non-decreasing: **YES** across `10` populated edge bins.

## Run-line side preference

- Games with both run-line sides priced/evaluated: `83`.
- Higher-EV side was `-1.5` in `32` games (`38.55%` of non-ties).
- Higher-EV side was `+1.5` in `51` games (`61.45%` of non-ties).
- Exact EV ties: `0`.

## Interpretation constraint

This report evaluates the candidate on the untouched test period only. The script does not refit, retune, or select hyperparameters from these results. Do not tune the model on this final test period after reviewing the report.

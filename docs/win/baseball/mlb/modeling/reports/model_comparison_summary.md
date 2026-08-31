# MLB Run Model Comparison

- Generated: `2026-08-31T13:34:13.231946+00:00`
- Untouched chronological test period: `2026-08-05` through `2026-08-30`
- Test games: `170`
- Model fitting/tuning performed by this evaluation script: `NO`
- Promotion status: `candidate_rejected`

## Production promotion gate

Candidate promotion requires mean Poisson deviance <= the DRatings baseline for BOTH home and away models.

| Side | DRatings baseline Poisson | Candidate Poisson | Candidate <= baseline |
| --- | --- | --- | --- |
| home | 2.130627 | 2.100216 | YES |
| away | 2.422857 | 2.521544 | NO |

- Production artifacts changed: **NO**.

## Run prediction metrics

| System | Side | Rows | MAE | Mean Poisson deviance | Mean predicted runs | Mean actual runs |
| --- | --- | --- | --- | --- | --- | --- |
| dratings | home | 170 | 2.270471 | 2.130627 | 4.361176 | 4.435294 |
| new_model | home | 170 | 2.253410 | 2.100216 | 4.393620 | 4.435294 |
| dratings | away | 170 | 2.436412 | 2.422857 | 4.164529 | 4.047059 |
| new_model | away | 170 | 2.537461 | 2.521544 | 4.426298 | 4.047059 |

### Run-prediction questions

- Does the new model improve home-run prediction error? **YES** (MAE `2.270471` -> `2.253410`; Poisson deviance `2.130627` -> `2.100216`).
- Does the new model improve away-run prediction error? **NO** (MAE `2.436412` -> `2.537461`; Poisson deviance `2.422857` -> `2.521544`).

## Probability calibration

Calibration YES/NO uses weighted expected calibration error (ECE) <= `0.05`. Totals use conditional win probability on resolved bets; pushes are excluded from the observed win-rate denominator.

| Market | New-model ECE | Calibrated | Predicted-vs-observed Spearman | Observed rate exactly non-decreasing | Populated bins |
| --- | --- | --- | --- | --- | --- |
| moneyline | 0.059120 | NO | 0.952381 | NO | 8 |
| run_line | 0.091671 | NO | 0.952381 | NO | 8 |
| total | 0.145866 | NO | -0.285714 | NO | 8 |

- Are predicted moneyline probabilities calibrated? **NO**.
- Are predicted run-line probabilities calibrated? **NO**.
- Are predicted total probabilities calibrated? **NO**.
- Does increasing predicted probability correspond to increasing observed win rate? Moneyline **NO**, run line **NO**, total **NO**. See Spearman values above for rank-direction strength.

## Probability log loss

| System | Market | Evaluation side | Rows | Log loss |
| --- | --- | --- | --- | --- |
| dratings | moneyline | home | 170 | 0.667212 |
| dratings | run_line | home | 170 | 0.694074 |
| dratings | total | over_resolved | 168 | 0.714864 |
| new_model | moneyline | home | 170 | 0.683315 |
| new_model | run_line | home | 170 | 0.684514 |
| new_model | total | over_resolved | 168 | 0.729005 |

## EV, realized return, and Kelly

- New-model priced candidates evaluated: `1020`; positive-EV candidates: `438`.
- New-model all-candidate mean predicted EV vs realized return: `-0.044638` vs `-0.044941`.
- New-model positive-EV mean predicted EV vs realized return: `0.154780` vs `-0.031735`.
- Does higher predicted EV correspond to higher realized return? EV/return Spearman = `0.001047`. A positive value indicates higher EV tended to correspond to higher realized return in this test sample.
- Is positive EV overstated versus realized return? **YES** (defined here as mean realized return below mean predicted EV among positive-EV candidates).
- DRatings-run baseline all-candidate mean predicted EV vs realized return: `-0.048445` vs `-0.044941`; EV/return Spearman `-0.093244`.
- Does Kelly increase monotonically with actual model edge? Edge/Kelly-raw Spearman = `0.994543`; mean raw Kelly across ordered edge bins is non-decreasing: **YES** across `10` populated edge bins.

## Run-line side preference

- Games with both run-line sides priced/evaluated: `170`.
- Higher-EV side was `-1.5` in `42` games (`24.71%` of non-ties).
- Higher-EV side was `+1.5` in `128` games (`75.29%` of non-ties).
- Exact EV ties: `0`.

## Interpretation constraint

This report evaluates the candidate on the untouched test period only. The script does not refit, retune, or select hyperparameters from these results. Do not tune the model on this final test period after reviewing the report.

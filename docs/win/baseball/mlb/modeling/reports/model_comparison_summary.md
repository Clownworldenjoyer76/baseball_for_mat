# MLB Run Model Comparison

- Generated: `2026-09-07T07:42:09.266540+00:00`
- Untouched chronological test period: `2026-08-11` through `2026-09-06`
- Test games: `163`
- Model fitting/tuning performed by this evaluation script: `NO`
- Promotion status: `candidate_rejected`

## Production promotion gate

Candidate promotion requires mean Poisson deviance <= the DRatings baseline for BOTH home and away models.

| Side | DRatings baseline Poisson | Candidate Poisson | Candidate <= baseline |
| --- | --- | --- | --- |
| home | 2.164962 | 2.285503 | NO |
| away | 2.524388 | 2.459367 | YES |

- Production artifacts changed: **NO**.

## Run prediction metrics

| System | Side | Rows | MAE | Mean Poisson deviance | Mean predicted runs | Mean actual runs |
| --- | --- | --- | --- | --- | --- | --- |
| dratings | home | 163 | 2.294110 | 2.164962 | 4.351779 | 4.509202 |
| new_model | home | 163 | 2.348746 | 2.285503 | 3.948876 | 4.509202 |
| dratings | away | 163 | 2.521656 | 2.524388 | 4.129877 | 4.263804 |
| new_model | away | 163 | 2.530360 | 2.459367 | 4.371460 | 4.263804 |

### Run-prediction questions

- Does the new model improve home-run prediction error? **NO** (MAE `2.294110` -> `2.348746`; Poisson deviance `2.164962` -> `2.285503`).
- Does the new model improve away-run prediction error? **NO** (MAE `2.521656` -> `2.530360`; Poisson deviance `2.524388` -> `2.459367`).

## Probability calibration

Calibration YES/NO uses weighted expected calibration error (ECE) <= `0.05`. Totals use conditional win probability on resolved bets; pushes are excluded from the observed win-rate denominator.

| Market | New-model ECE | Calibrated | Predicted-vs-observed Spearman | Observed rate exactly non-decreasing | Populated bins |
| --- | --- | --- | --- | --- | --- |
| moneyline | 0.135285 | NO | -0.054545 | NO | 10 |
| run_line | 0.085461 | NO | 0.975758 | NO | 10 |
| total | 0.136713 | NO | -0.284848 | NO | 10 |

- Are predicted moneyline probabilities calibrated? **NO**.
- Are predicted run-line probabilities calibrated? **NO**.
- Are predicted total probabilities calibrated? **NO**.
- Does increasing predicted probability correspond to increasing observed win rate? Moneyline **NO**, run line **NO**, total **NO**. See Spearman values above for rank-direction strength.

## Probability log loss

| System | Market | Evaluation side | Rows | Log loss |
| --- | --- | --- | --- | --- |
| dratings | moneyline | home | 163 | 0.679955 |
| dratings | run_line | home | 162 | 0.687701 |
| dratings | total | over_resolved | 159 | 0.718028 |
| new_model | moneyline | home | 163 | 0.673881 |
| new_model | run_line | home | 162 | 0.654154 |
| new_model | total | over_resolved | 159 | 0.734485 |

## EV, realized return, and Kelly

- New-model priced candidates evaluated: `976`; positive-EV candidates: `422`.
- New-model all-candidate mean predicted EV vs realized return: `-0.043830` vs `-0.045287`.
- New-model positive-EV mean predicted EV vs realized return: `0.206825` vs `0.061919`.
- Does higher predicted EV correspond to higher realized return? EV/return Spearman = `0.099615`. A positive value indicates higher EV tended to correspond to higher realized return in this test sample.
- Is positive EV overstated versus realized return? **YES** (defined here as mean realized return below mean predicted EV among positive-EV candidates).
- DRatings-run baseline all-candidate mean predicted EV vs realized return: `-0.049525` vs `-0.045287`; EV/return Spearman `-0.116689`.
- Does Kelly increase monotonically with actual model edge? Edge/Kelly-raw Spearman = `0.994830`; mean raw Kelly across ordered edge bins is non-decreasing: **YES** across `10` populated edge bins.

## Run-line side preference

- Games with both run-line sides priced/evaluated: `162`.
- Higher-EV side was `-1.5` in `43` games (`26.54%` of non-ties).
- Higher-EV side was `+1.5` in `119` games (`73.46%` of non-ties).
- Exact EV ties: `0`.

## Interpretation constraint

This report evaluates the candidate on the untouched test period only. The script does not refit, retune, or select hyperparameters from these results. Do not tune the model on this final test period after reviewing the report.

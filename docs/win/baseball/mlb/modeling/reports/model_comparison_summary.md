# MLB Run Model Comparison

- Generated: `2026-08-30T15:47:02.863003+00:00`
- Untouched chronological test period: `2026-08-03` through `2026-08-29`
- Test games: `127`
- Model fitting/tuning performed by this evaluation script: `NO`

## Run prediction metrics

| System | Side | Rows | MAE | Mean Poisson deviance | Mean predicted runs | Mean actual runs |
| --- | --- | --- | --- | --- | --- | --- |
| dratings | home | 127 | 2.311654 | 2.226090 | 4.341732 | 4.440945 |
| new_model | home | 127 | 2.357172 | 2.324772 | 4.378084 | 4.440945 |
| dratings | away | 127 | 2.429370 | 2.374508 | 4.175984 | 3.866142 |
| new_model | away | 127 | 2.498517 | 2.457037 | 4.368808 | 3.866142 |

### Run-prediction questions

- Does the new model improve home-run prediction error? **NO** (MAE `2.311654` -> `2.357172`; Poisson deviance `2.226090` -> `2.324772`).
- Does the new model improve away-run prediction error? **NO** (MAE `2.429370` -> `2.498517`; Poisson deviance `2.374508` -> `2.457037`).

## Probability calibration

Calibration YES/NO uses weighted expected calibration error (ECE) <= `0.05`. Totals use conditional win probability on resolved bets; pushes are excluded from the observed win-rate denominator.

| Market | New-model ECE | Calibrated | Predicted-vs-observed Spearman | Observed rate exactly non-decreasing | Populated bins |
| --- | --- | --- | --- | --- | --- |
| moneyline | 0.110369 | NO | -0.018182 | NO | 10 |
| run_line | 0.117804 | NO | 0.785714 | NO | 8 |
| total | 0.162102 | NO | 0.000000 | NO | 8 |

- Are predicted moneyline probabilities calibrated? **NO**.
- Are predicted run-line probabilities calibrated? **NO**.
- Are predicted total probabilities calibrated? **NO**.
- Does increasing predicted probability correspond to increasing observed win rate? Moneyline **NO**, run line **NO**, total **NO**. See Spearman values above for rank-direction strength.

## Probability log loss

| System | Market | Evaluation side | Rows | Log loss |
| --- | --- | --- | --- | --- |
| dratings | moneyline | home | 127 | 0.686577 |
| dratings | run_line | home | 127 | 0.677242 |
| dratings | total | over_resolved | 125 | 0.729623 |
| new_model | moneyline | home | 127 | 0.735763 |
| new_model | run_line | home | 127 | 0.717505 |
| new_model | total | over_resolved | 125 | 0.735655 |

## EV, realized return, and Kelly

- New-model priced candidates evaluated: `762`; positive-EV candidates: `334`.
- New-model all-candidate mean predicted EV vs realized return: `-0.047821` vs `-0.046680`.
- New-model positive-EV mean predicted EV vs realized return: `0.177373` vs `-0.123353`.
- Does higher predicted EV correspond to higher realized return? EV/return Spearman = `-0.068679`. A positive value indicates higher EV tended to correspond to higher realized return in this test sample.
- Is positive EV overstated versus realized return? **YES** (defined here as mean realized return below mean predicted EV among positive-EV candidates).
- DRatings-run baseline all-candidate mean predicted EV vs realized return: `-0.050241` vs `-0.046680`; EV/return Spearman `-0.125288`.
- Does Kelly increase monotonically with actual model edge? Edge/Kelly-raw Spearman = `0.993311`; mean raw Kelly across ordered edge bins is non-decreasing: **YES** across `10` populated edge bins.

## Run-line side preference

- Games with both run-line sides priced/evaluated: `127`.
- Higher-EV side was `-1.5` in `45` games (`35.43%` of non-ties).
- Higher-EV side was `+1.5` in `82` games (`64.57%` of non-ties).
- Exact EV ties: `0`.

## Interpretation constraint

This report evaluates the saved model on the untouched test period only. The script does not refit, retune, or select hyperparameters from these results. Do not tune the model on this final test period after reviewing the report.

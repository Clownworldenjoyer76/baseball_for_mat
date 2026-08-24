# MLB Run Model Comparison

- Generated: `2026-08-24T14:23:47.973189+00:00`
- Untouched chronological test period: `2026-07-29` through `2026-08-19`
- Test games: `144`
- Model fitting/tuning performed by this evaluation script: `NO`

## Run prediction metrics

| System | Side | Rows | MAE | Mean Poisson deviance | Mean predicted runs | Mean actual runs |
| --- | --- | --- | --- | --- | --- | --- |
| dratings | home | 144 | 2.445556 | 2.437353 | 4.484306 | 4.472222 |
| new_model | home | 144 | 2.409228 | 2.384706 | 4.605406 | 4.472222 |
| dratings | away | 144 | 2.193681 | 2.024379 | 4.165764 | 3.673611 |
| new_model | away | 144 | 2.313268 | 2.161019 | 4.506166 | 3.673611 |

### Run-prediction questions

- Does the new model improve home-run prediction error? **YES** (MAE `2.445556` -> `2.409228`; Poisson deviance `2.437353` -> `2.384706`).
- Does the new model improve away-run prediction error? **NO** (MAE `2.193681` -> `2.313268`; Poisson deviance `2.024379` -> `2.161019`).

## Probability calibration

Calibration YES/NO uses weighted expected calibration error (ECE) <= `0.05`. Totals use conditional win probability on resolved bets; pushes are excluded from the observed win-rate denominator.

| Market | New-model ECE | Calibrated | Predicted-vs-observed Spearman | Observed rate exactly non-decreasing | Populated bins |
| --- | --- | --- | --- | --- | --- |
| moneyline | 0.129438 | NO | -0.428571 | NO | 8 |
| run_line | 0.074025 | NO | 0.857143 | NO | 8 |
| total | 0.200975 | NO | 0.380952 | NO | 8 |

- Are predicted moneyline probabilities calibrated? **NO**.
- Are predicted run-line probabilities calibrated? **NO**.
- Are predicted total probabilities calibrated? **NO**.
- Does increasing predicted probability correspond to increasing observed win rate? Moneyline **NO**, run line **NO**, total **NO**. See Spearman values above for rank-direction strength.

## Probability log loss

| System | Market | Evaluation side | Rows | Log loss |
| --- | --- | --- | --- | --- |
| dratings | moneyline | home | 144 | 0.684945 |
| dratings | run_line | home | 144 | 0.682088 |
| dratings | total | over_resolved | 142 | 0.715542 |
| new_model | moneyline | home | 144 | 0.707712 |
| new_model | run_line | home | 144 | 0.690982 |
| new_model | total | over_resolved | 142 | 0.748776 |

## EV, realized return, and Kelly

- New-model priced candidates evaluated: `864`; positive-EV candidates: `359`.
- New-model all-candidate mean predicted EV vs realized return: `-0.048202` vs `-0.044062`.
- New-model positive-EV mean predicted EV vs realized return: `0.177778` vs `0.026128`.
- Does higher predicted EV correspond to higher realized return? EV/return Spearman = `0.011275`. A positive value indicates higher EV tended to correspond to higher realized return in this test sample.
- Is positive EV overstated versus realized return? **YES** (defined here as mean realized return below mean predicted EV among positive-EV candidates).
- DRatings-run baseline all-candidate mean predicted EV vs realized return: `-0.051520` vs `-0.044062`; EV/return Spearman `-0.039638`.
- Does Kelly increase monotonically with actual model edge? Edge/Kelly-raw Spearman = `0.994795`; mean raw Kelly across ordered edge bins is non-decreasing: **YES** across `10` populated edge bins.

## Run-line side preference

- Games with both run-line sides priced/evaluated: `144`.
- Higher-EV side was `-1.5` in `50` games (`34.72%` of non-ties).
- Higher-EV side was `+1.5` in `94` games (`65.28%` of non-ties).
- Exact EV ties: `0`.

## Interpretation constraint

This report evaluates the saved model on the untouched test period only. The script does not refit, retune, or select hyperparameters from these results. Do not tune the model on this final test period after reviewing the report.

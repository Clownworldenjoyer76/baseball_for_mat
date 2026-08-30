# MLB Run Model Comparison

- Generated: `2026-08-30T10:51:52.426816+00:00`
- Untouched chronological test period: `2026-08-02` through `2026-08-28`
- Test games: `148`
- Model fitting/tuning performed by this evaluation script: `NO`

## Run prediction metrics

| System | Side | Rows | MAE | Mean Poisson deviance | Mean predicted runs | Mean actual runs |
| --- | --- | --- | --- | --- | --- | --- |
| dratings | home | 148 | 2.401216 | 2.355752 | 4.355541 | 4.452703 |
| new_model | home | 148 | 2.432256 | 2.377705 | 4.352013 | 4.452703 |
| dratings | away | 148 | 2.436419 | 2.418880 | 4.135338 | 3.777027 |
| new_model | away | 148 | 2.531932 | 2.549702 | 4.386795 | 3.777027 |

### Run-prediction questions

- Does the new model improve home-run prediction error? **NO** (MAE `2.401216` -> `2.432256`; Poisson deviance `2.355752` -> `2.377705`).
- Does the new model improve away-run prediction error? **NO** (MAE `2.436419` -> `2.531932`; Poisson deviance `2.418880` -> `2.549702`).

## Probability calibration

Calibration YES/NO uses weighted expected calibration error (ECE) <= `0.05`. Totals use conditional win probability on resolved bets; pushes are excluded from the observed win-rate denominator.

| Market | New-model ECE | Calibrated | Predicted-vs-observed Spearman | Observed rate exactly non-decreasing | Populated bins |
| --- | --- | --- | --- | --- | --- |
| moneyline | 0.139952 | NO | 0.000000 | NO | 8 |
| run_line | 0.140770 | NO | 0.952381 | NO | 8 |
| total | 0.161198 | NO | -0.428571 | NO | 8 |

- Are predicted moneyline probabilities calibrated? **NO**.
- Are predicted run-line probabilities calibrated? **NO**.
- Are predicted total probabilities calibrated? **NO**.
- Does increasing predicted probability correspond to increasing observed win rate? Moneyline **NO**, run line **NO**, total **NO**. See Spearman values above for rank-direction strength.

## Probability log loss

| System | Market | Evaluation side | Rows | Log loss |
| --- | --- | --- | --- | --- |
| dratings | moneyline | home | 148 | 0.686609 |
| dratings | run_line | home | 148 | 0.691337 |
| dratings | total | over_resolved | 146 | 0.728035 |
| new_model | moneyline | home | 148 | 0.713662 |
| new_model | run_line | home | 148 | 0.702708 |
| new_model | total | over_resolved | 146 | 0.741448 |

## EV, realized return, and Kelly

- New-model priced candidates evaluated: `888`; positive-EV candidates: `386`.
- New-model all-candidate mean predicted EV vs realized return: `-0.045727` vs `-0.043153`.
- New-model positive-EV mean predicted EV vs realized return: `0.171120` vs `-0.081269`.
- Does higher predicted EV correspond to higher realized return? EV/return Spearman = `-0.029171`. A positive value indicates higher EV tended to correspond to higher realized return in this test sample.
- Is positive EV overstated versus realized return? **YES** (defined here as mean realized return below mean predicted EV among positive-EV candidates).
- DRatings-run baseline all-candidate mean predicted EV vs realized return: `-0.049847` vs `-0.043153`; EV/return Spearman `-0.139358`.
- Does Kelly increase monotonically with actual model edge? Edge/Kelly-raw Spearman = `0.994596`; mean raw Kelly across ordered edge bins is non-decreasing: **YES** across `10` populated edge bins.

## Run-line side preference

- Games with both run-line sides priced/evaluated: `148`.
- Higher-EV side was `-1.5` in `52` games (`35.14%` of non-ties).
- Higher-EV side was `+1.5` in `96` games (`64.86%` of non-ties).
- Exact EV ties: `0`.

## Interpretation constraint

This report evaluates the saved model on the untouched test period only. The script does not refit, retune, or select hyperparameters from these results. Do not tune the model on this final test period after reviewing the report.

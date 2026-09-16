# MLB Run Model Comparison

- Generated: `2026-09-16T07:43:00.043739+00:00`
- Untouched chronological test period: `2026-08-18` through `2026-09-15`
- Test games: `151`
- Model fitting/tuning performed by this evaluation script: `NO`
- Promotion status: `candidate_rejected`

## Production promotion gate

Candidate promotion requires mean Poisson deviance <= the DRatings baseline for BOTH home and away models.

| Side | DRatings baseline Poisson | Candidate Poisson | Candidate <= baseline |
| --- | --- | --- | --- |
| home | 2.300855 | 2.462090 | NO |
| away | 2.577849 | 2.526234 | YES |

- Production artifacts changed: **NO**.

## Run prediction metrics

| System | Side | Rows | MAE | Mean Poisson deviance | Mean predicted runs | Mean actual runs |
| --- | --- | --- | --- | --- | --- | --- |
| dratings | home | 151 | 2.375563 | 2.300855 | 4.330795 | 4.509934 |
| new_model | home | 151 | 2.414744 | 2.462090 | 4.185360 | 4.509934 |
| dratings | away | 151 | 2.549735 | 2.577849 | 4.141921 | 4.463576 |
| new_model | away | 151 | 2.608451 | 2.526234 | 4.249050 | 4.463576 |

### Run-prediction questions

- Does the new model improve home-run prediction error? **NO** (MAE `2.375563` -> `2.414744`; Poisson deviance `2.300855` -> `2.462090`).
- Does the new model improve away-run prediction error? **NO** (MAE `2.549735` -> `2.608451`; Poisson deviance `2.577849` -> `2.526234`).

## Probability calibration

Calibration YES/NO uses weighted expected calibration error (ECE) <= `0.05`. Totals use conditional win probability on resolved bets; pushes are excluded from the observed win-rate denominator.

| Market | New-model ECE | Calibrated | Predicted-vs-observed Spearman | Observed rate exactly non-decreasing | Populated bins |
| --- | --- | --- | --- | --- | --- |
| moneyline | 0.067690 | NO | 0.761905 | NO | 8 |
| run_line | 0.120121 | NO | 0.369697 | NO | 10 |
| total | 0.137829 | NO | -0.103030 | NO | 10 |

- Are predicted moneyline probabilities calibrated? **NO**.
- Are predicted run-line probabilities calibrated? **NO**.
- Are predicted total probabilities calibrated? **NO**.
- Does increasing predicted probability correspond to increasing observed win rate? Moneyline **NO**, run line **NO**, total **NO**. See Spearman values above for rank-direction strength.

## Probability log loss

| System | Market | Evaluation side | Rows | Log loss |
| --- | --- | --- | --- | --- |
| dratings | moneyline | home | 151 | 0.672356 |
| dratings | run_line | home | 150 | 0.691650 |
| dratings | total | over_resolved | 147 | 0.718310 |
| new_model | moneyline | home | 151 | 0.702168 |
| new_model | run_line | home | 150 | 0.720566 |
| new_model | total | over_resolved | 147 | 0.741569 |

## EV, realized return, and Kelly

- New-model priced candidates evaluated: `904`; positive-EV candidates: `412`.
- New-model all-candidate mean predicted EV vs realized return: `-0.042164` vs `-0.044060`.
- New-model positive-EV mean predicted EV vs realized return: `0.235378` vs `0.014927`.
- Does higher predicted EV correspond to higher realized return? EV/return Spearman = `0.052717`. A positive value indicates higher EV tended to correspond to higher realized return in this test sample.
- Is positive EV overstated versus realized return? **YES** (defined here as mean realized return below mean predicted EV among positive-EV candidates).
- DRatings-run baseline all-candidate mean predicted EV vs realized return: `-0.049460` vs `-0.044060`; EV/return Spearman `-0.111459`.
- Does Kelly increase monotonically with actual model edge? Edge/Kelly-raw Spearman = `0.993710`; mean raw Kelly across ordered edge bins is non-decreasing: **YES** across `10` populated edge bins.

## Run-line side preference

- Games with both run-line sides priced/evaluated: `150`.
- Higher-EV side was `-1.5` in `41` games (`27.33%` of non-ties).
- Higher-EV side was `+1.5` in `109` games (`72.67%` of non-ties).
- Exact EV ties: `0`.

## Interpretation constraint

This report evaluates the candidate on the untouched test period only. The script does not refit, retune, or select hyperparameters from these results. Do not tune the model on this final test period after reviewing the report.

# MLB Run Model Comparison

- Generated: `2026-09-11T07:43:11.553055+00:00`
- Untouched chronological test period: `2026-08-14` through `2026-09-10`
- Test games: `157`
- Model fitting/tuning performed by this evaluation script: `NO`
- Promotion status: `candidate_rejected`

## Production promotion gate

Candidate promotion requires mean Poisson deviance <= the DRatings baseline for BOTH home and away models.

| Side | DRatings baseline Poisson | Candidate Poisson | Candidate <= baseline |
| --- | --- | --- | --- |
| home | 2.272537 | 2.314398 | NO |
| away | 2.460105 | 2.637425 | NO |

- Production artifacts changed: **NO**.

## Run prediction metrics

| System | Side | Rows | MAE | Mean Poisson deviance | Mean predicted runs | Mean actual runs |
| --- | --- | --- | --- | --- | --- | --- |
| dratings | home | 157 | 2.371274 | 2.272537 | 4.352930 | 4.535032 |
| new_model | home | 157 | 2.416194 | 2.314398 | 4.343187 | 4.535032 |
| dratings | away | 157 | 2.489490 | 2.460105 | 4.107325 | 4.331210 |
| new_model | away | 157 | 2.549049 | 2.637425 | 3.909948 | 4.331210 |

### Run-prediction questions

- Does the new model improve home-run prediction error? **NO** (MAE `2.371274` -> `2.416194`; Poisson deviance `2.272537` -> `2.314398`).
- Does the new model improve away-run prediction error? **NO** (MAE `2.489490` -> `2.549049`; Poisson deviance `2.460105` -> `2.637425`).

## Probability calibration

Calibration YES/NO uses weighted expected calibration error (ECE) <= `0.05`. Totals use conditional win probability on resolved bets; pushes are excluded from the observed win-rate denominator.

| Market | New-model ECE | Calibrated | Predicted-vs-observed Spearman | Observed rate exactly non-decreasing | Populated bins |
| --- | --- | --- | --- | --- | --- |
| moneyline | 0.115311 | NO | 0.781818 | NO | 10 |
| run_line | 0.128214 | NO | 0.833333 | NO | 8 |
| total | 0.139975 | NO | -0.738095 | NO | 8 |

- Are predicted moneyline probabilities calibrated? **NO**.
- Are predicted run-line probabilities calibrated? **NO**.
- Are predicted total probabilities calibrated? **NO**.
- Does increasing predicted probability correspond to increasing observed win rate? Moneyline **NO**, run line **NO**, total **NO**. See Spearman values above for rank-direction strength.

## Probability log loss

| System | Market | Evaluation side | Rows | Log loss |
| --- | --- | --- | --- | --- |
| dratings | moneyline | home | 157 | 0.664394 |
| dratings | run_line | home | 156 | 0.690711 |
| dratings | total | over_resolved | 153 | 0.721636 |
| new_model | moneyline | home | 157 | 0.706637 |
| new_model | run_line | home | 156 | 0.728933 |
| new_model | total | over_resolved | 153 | 0.752357 |

## EV, realized return, and Kelly

- New-model priced candidates evaluated: `940`; positive-EV candidates: `435`.
- New-model all-candidate mean predicted EV vs realized return: `-0.047490` vs `-0.048106`.
- New-model positive-EV mean predicted EV vs realized return: `0.218705` vs `-0.057264`.
- Does higher predicted EV correspond to higher realized return? EV/return Spearman = `-0.023960`. A positive value indicates higher EV tended to correspond to higher realized return in this test sample.
- Is positive EV overstated versus realized return? **YES** (defined here as mean realized return below mean predicted EV among positive-EV candidates).
- DRatings-run baseline all-candidate mean predicted EV vs realized return: `-0.049697` vs `-0.048106`; EV/return Spearman `-0.117699`.
- Does Kelly increase monotonically with actual model edge? Edge/Kelly-raw Spearman = `0.992016`; mean raw Kelly across ordered edge bins is non-decreasing: **YES** across `10` populated edge bins.

## Run-line side preference

- Games with both run-line sides priced/evaluated: `156`.
- Higher-EV side was `-1.5` in `59` games (`37.82%` of non-ties).
- Higher-EV side was `+1.5` in `97` games (`62.18%` of non-ties).
- Exact EV ties: `0`.

## Interpretation constraint

This report evaluates the candidate on the untouched test period only. The script does not refit, retune, or select hyperparameters from these results. Do not tune the model on this final test period after reviewing the report.

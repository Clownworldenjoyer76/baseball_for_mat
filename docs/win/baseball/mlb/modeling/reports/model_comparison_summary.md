# MLB Run Model Comparison

- Generated: `2026-09-12T13:31:09.116753+00:00`
- Untouched chronological test period: `2026-08-14` through `2026-09-10`
- Test games: `157`
- Model fitting/tuning performed by this evaluation script: `NO`
- Promotion status: `candidate_rejected`

## Production promotion gate

Candidate promotion requires the coupled mean home/away Poisson deviance to meet or beat DRatings AND each probability market to pass both relative and absolute log-loss/ECE thresholds.

| Run metric | DRatings | Candidate | Candidate <= baseline |
| --- | --- | --- | --- |
| home Poisson (diagnostic) | 2.272537 | 2.318701 | NO |
| away Poisson (diagnostic) | 2.460105 | 2.434801 | YES |
| coupled mean Poisson (GATE) | 2.366321 | 2.376751 | NO |

| Probability market gate | DRatings log loss | Candidate log loss | Log-loss ceiling | LL <= baseline | LL <= ceiling | DRatings ECE | Candidate ECE | ECE ceiling | ECE <= baseline | ECE <= ceiling | Market PASS |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| moneyline | 0.664394 | 0.682398 | 0.670000 | NO | NO | 0.096429 | 0.081736 | 0.050000 | YES | NO | NO |
| run_line | 0.690711 | 0.704978 | 0.660000 | NO | NO | 0.080527 | 0.131948 | 0.050000 | NO | NO | NO |
| total | 0.721636 | 0.725276 | 0.670000 | NO | NO | 0.060940 | 0.094191 | 0.050000 | NO | NO | NO |

- Production artifacts changed: **NO**.

## Run prediction metrics

| System | Side | Rows | MAE | Mean Poisson deviance | Mean predicted runs | Mean actual runs |
| --- | --- | --- | --- | --- | --- | --- |
| dratings | home | 157 | 2.371274 | 2.272537 | 4.352930 | 4.535032 |
| new_model | home | 157 | 2.427674 | 2.318701 | 4.402977 | 4.535032 |
| dratings | away | 157 | 2.489490 | 2.460105 | 4.107325 | 4.331210 |
| new_model | away | 157 | 2.512010 | 2.434801 | 4.217389 | 4.331210 |

### Run-prediction questions

- Does the new model improve home-run prediction error? **NO** (MAE `2.371274` -> `2.427674`; Poisson deviance `2.272537` -> `2.318701`).
- Does the new model improve away-run prediction error? **NO** (MAE `2.489490` -> `2.512010`; Poisson deviance `2.460105` -> `2.434801`).

## Probability calibration

Calibration YES/NO uses the configured per-market weighted expected calibration error (ECE) ceiling. All current market ECE ceilings are `0.05`. Totals use conditional win probability on resolved bets; pushes are excluded from the observed win-rate denominator.

| Market | New-model ECE | Calibrated | Predicted-vs-observed Spearman | Observed rate exactly non-decreasing | Populated bins |
| --- | --- | --- | --- | --- | --- |
| moneyline | 0.081736 | NO | 0.857143 | NO | 8 |
| run_line | 0.131948 | NO | 0.476190 | NO | 8 |
| total | 0.094191 | NO | -0.347863 | NO | 6 |

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
| new_model | moneyline | home | 157 | 0.682398 |
| new_model | run_line | home | 156 | 0.704978 |
| new_model | total | over_resolved | 153 | 0.725276 |

## EV, realized return, and Kelly

- New-model priced candidates evaluated: `940`; positive-EV candidates: `409`.
- New-model all-candidate mean predicted EV vs realized return: `-0.044454` vs `-0.048160`.
- New-model positive-EV mean predicted EV vs realized return: `0.173397` vs `-0.011027`.
- Does higher predicted EV correspond to higher realized return? EV/return Spearman = `0.001469`. A positive value indicates higher EV tended to correspond to higher realized return in this test sample.
- Is positive EV overstated versus realized return? **YES** (defined here as mean realized return below mean predicted EV among positive-EV candidates).
- DRatings-run baseline all-candidate mean predicted EV vs realized return: `-0.049703` vs `-0.048160`; EV/return Spearman `-0.118280`.
- Does Kelly increase monotonically with actual model edge? Edge/Kelly-raw Spearman = `0.993731`; mean raw Kelly across ordered edge bins is non-decreasing: **YES** across `10` populated edge bins.

## Run-line side preference

- Games with both run-line sides priced/evaluated: `156`.
- Higher-EV side was `-1.5` in `41` games (`26.28%` of non-ties).
- Higher-EV side was `+1.5` in `115` games (`73.72%` of non-ties).
- Exact EV ties: `0`.

## Interpretation constraint

This report evaluates the candidate on the untouched test period only. The script does not refit, retune, or select hyperparameters from these results. Do not tune the model on this final test period after reviewing the report.

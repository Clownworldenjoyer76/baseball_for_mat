# MLB Run Model Comparison

- Generated: `2026-10-01T14:35:49.056050+00:00`
- Untouched chronological test period: `2026-09-02` through `2026-09-30`
- Test games: `85`
- Model fitting/tuning performed by this evaluation script: `NO`
- Promotion status: `candidate_rejected`

## Production promotion gate

Candidate promotion requires mean Poisson deviance <= the DRatings baseline for BOTH home and away models.

| Side | DRatings baseline Poisson | Candidate Poisson | Candidate <= baseline |
| --- | --- | --- | --- |
| home | 1.880364 | 2.010967 | NO |
| away | 2.152882 | 2.020355 | YES |

- Production artifacts changed: **NO**.

## Run prediction metrics

| System | Side | Rows | MAE | Mean Poisson deviance | Mean predicted runs | Mean actual runs |
| --- | --- | --- | --- | --- | --- | --- |
| dratings | home | 85 | 2.245765 | 1.880364 | 4.302706 | 4.517647 |
| new_model | home | 85 | 2.338186 | 2.010967 | 4.386761 | 4.517647 |
| dratings | away | 85 | 2.380235 | 2.152882 | 4.035529 | 4.964706 |
| new_model | away | 85 | 2.363038 | 2.020355 | 4.256159 | 4.964706 |

### Run-prediction questions

- Does the new model improve home-run prediction error? **NO** (MAE `2.245765` -> `2.338186`; Poisson deviance `1.880364` -> `2.010967`).
- Does the new model improve away-run prediction error? **YES** (MAE `2.380235` -> `2.363038`; Poisson deviance `2.152882` -> `2.020355`).

## Probability calibration

Calibration YES/NO uses weighted expected calibration error (ECE) <= `0.05`. Totals use conditional win probability on resolved bets; pushes are excluded from the observed win-rate denominator.

| Market | New-model ECE | Calibrated | Predicted-vs-observed Spearman | Observed rate exactly non-decreasing | Populated bins |
| --- | --- | --- | --- | --- | --- |
| moneyline | 0.094593 | NO | -0.166667 | NO | 8 |
| run_line | 0.156890 | NO | 0.938591 | YES | 8 |
| total | 0.100597 | NO | -0.190476 | NO | 8 |

- Are predicted moneyline probabilities calibrated? **NO**.
- Are predicted run-line probabilities calibrated? **NO**.
- Are predicted total probabilities calibrated? **NO**.
- Does increasing predicted probability correspond to increasing observed win rate? Moneyline **NO**, run line **YES**, total **NO**. See Spearman values above for rank-direction strength.

## Probability log loss

| System | Market | Evaluation side | Rows | Log loss |
| --- | --- | --- | --- | --- |
| dratings | moneyline | home | 85 | 0.642360 |
| dratings | run_line | home | 83 | 0.698394 |
| dratings | total | over_resolved | 82 | 0.713023 |
| new_model | moneyline | home | 85 | 0.679597 |
| new_model | run_line | home | 83 | 0.725808 |
| new_model | total | over_resolved | 82 | 0.696399 |

## EV, realized return, and Kelly

- New-model priced candidates evaluated: `506`; positive-EV candidates: `211`.
- New-model all-candidate mean predicted EV vs realized return: `-0.044846` vs `-0.054486`.
- New-model positive-EV mean predicted EV vs realized return: `0.166269` vs `-0.114882`.
- Does higher predicted EV correspond to higher realized return? EV/return Spearman = `-0.034803`. A positive value indicates higher EV tended to correspond to higher realized return in this test sample.
- Is positive EV overstated versus realized return? **YES** (defined here as mean realized return below mean predicted EV among positive-EV candidates).
- DRatings-run baseline all-candidate mean predicted EV vs realized return: `-0.056856` vs `-0.054486`; EV/return Spearman `-0.098249`.
- Does Kelly increase monotonically with actual model edge? Edge/Kelly-raw Spearman = `0.992580`; mean raw Kelly across ordered edge bins is non-decreasing: **YES** across `10` populated edge bins.

## Run-line side preference

- Games with both run-line sides priced/evaluated: `83`.
- Higher-EV side was `-1.5` in `20` games (`24.10%` of non-ties).
- Higher-EV side was `+1.5` in `63` games (`75.90%` of non-ties).
- Exact EV ties: `0`.

## Interpretation constraint

This report evaluates the candidate on the untouched test period only. The script does not refit, retune, or select hyperparameters from these results. Do not tune the model on this final test period after reviewing the report.

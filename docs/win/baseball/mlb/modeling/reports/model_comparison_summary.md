# MLB Run Model Comparison

- Generated: `2026-10-01T07:43:01.852730+00:00`
- Untouched chronological test period: `2026-09-02` through `2026-09-30`
- Test games: `87`
- Model fitting/tuning performed by this evaluation script: `NO`
- Promotion status: `candidate_rejected`

## Production promotion gate

Candidate promotion requires mean Poisson deviance <= the DRatings baseline for BOTH home and away models.

| Side | DRatings baseline Poisson | Candidate Poisson | Candidate <= baseline |
| --- | --- | --- | --- |
| home | 1.842549 | 2.050222 | NO |
| away | 2.135354 | 2.308734 | NO |

- Production artifacts changed: **NO**.

## Run prediction metrics

| System | Side | Rows | MAE | Mean Poisson deviance | Mean predicted runs | Mean actual runs |
| --- | --- | --- | --- | --- | --- | --- |
| dratings | home | 87 | 2.214138 | 1.842549 | 4.292759 | 4.482759 |
| new_model | home | 87 | 2.324244 | 2.050222 | 4.356151 | 4.482759 |
| dratings | away | 87 | 2.370230 | 2.135354 | 4.024483 | 4.977011 |
| new_model | away | 87 | 2.441243 | 2.308734 | 3.505976 | 4.977011 |

### Run-prediction questions

- Does the new model improve home-run prediction error? **NO** (MAE `2.214138` -> `2.324244`; Poisson deviance `1.842549` -> `2.050222`).
- Does the new model improve away-run prediction error? **NO** (MAE `2.370230` -> `2.441243`; Poisson deviance `2.135354` -> `2.308734`).

## Probability calibration

Calibration YES/NO uses weighted expected calibration error (ECE) <= `0.05`. Totals use conditional win probability on resolved bets; pushes are excluded from the observed win-rate denominator.

| Market | New-model ECE | Calibrated | Predicted-vs-observed Spearman | Observed rate exactly non-decreasing | Populated bins |
| --- | --- | --- | --- | --- | --- |
| moneyline | 0.190973 | NO | 0.899700 | NO | 10 |
| run_line | 0.220594 | NO | 0.103030 | NO | 10 |
| total | 0.221648 | NO | -0.275454 | NO | 8 |

- Are predicted moneyline probabilities calibrated? **NO**.
- Are predicted run-line probabilities calibrated? **NO**.
- Are predicted total probabilities calibrated? **NO**.
- Does increasing predicted probability correspond to increasing observed win rate? Moneyline **NO**, run line **NO**, total **NO**. See Spearman values above for rank-direction strength.

## Probability log loss

| System | Market | Evaluation side | Rows | Log loss |
| --- | --- | --- | --- | --- |
| dratings | moneyline | home | 87 | 0.646266 |
| dratings | run_line | home | 85 | 0.691829 |
| dratings | total | over_resolved | 83 | 0.712712 |
| new_model | moneyline | home | 87 | 0.775006 |
| new_model | run_line | home | 85 | 0.793067 |
| new_model | total | over_resolved | 83 | 0.788281 |

## EV, realized return, and Kelly

- New-model priced candidates evaluated: `518`; positive-EV candidates: `234`.
- New-model all-candidate mean predicted EV vs realized return: `-0.050289` vs `-0.054865`.
- New-model positive-EV mean predicted EV vs realized return: `0.282446` vs `-0.095983`.
- Does higher predicted EV correspond to higher realized return? EV/return Spearman = `-0.083047`. A positive value indicates higher EV tended to correspond to higher realized return in this test sample.
- Is positive EV overstated versus realized return? **YES** (defined here as mean realized return below mean predicted EV among positive-EV candidates).
- DRatings-run baseline all-candidate mean predicted EV vs realized return: `-0.056773` vs `-0.054865`; EV/return Spearman `-0.095696`.
- Does Kelly increase monotonically with actual model edge? Edge/Kelly-raw Spearman = `0.989357`; mean raw Kelly across ordered edge bins is non-decreasing: **YES** across `10` populated edge bins.

## Run-line side preference

- Games with both run-line sides priced/evaluated: `85`.
- Higher-EV side was `-1.5` in `37` games (`43.53%` of non-ties).
- Higher-EV side was `+1.5` in `48` games (`56.47%` of non-ties).
- Exact EV ties: `0`.

## Interpretation constraint

This report evaluates the candidate on the untouched test period only. The script does not refit, retune, or select hyperparameters from these results. Do not tune the model on this final test period after reviewing the report.

# Probability Calibration

Generated: `2026-09-14T10:39:00.590204+00:00`

Calibration source: chronological OOS Poisson/Skellam predictions from item #6.

Production calibrators are fit on all four OOS CV folds. The `final_test_reference`
row is evaluation only and is not used to fit the production calibrators.

## Metrics

| market    | period               |   observations |   calibrator_train_observations |   raw_log_loss |   calibrated_log_loss |   log_loss_delta_cal_minus_raw |   raw_ece |   calibrated_ece |   ece_delta_cal_minus_raw |
|:----------|:---------------------|---------------:|--------------------------------:|---------------:|----------------------:|-------------------------------:|----------:|-----------------:|--------------------------:|
| moneyline | cv_fold_2            |            224 |                             196 |       0.673354 |              0.688664 |                     0.0153093  | 0.0393776 |        0.0833557 |                0.0439782  |
| moneyline | cv_fold_3            |            192 |                             420 |       0.695194 |              0.693907 |                    -0.00128668 | 0.0310675 |        0.0194486 |               -0.0116189  |
| moneyline | cv_fold_4            |            145 |                             612 |       0.695127 |              0.69321  |                    -0.001917   | 0.0953428 |        0.06514   |               -0.0302028  |
| moneyline | crossfit_combined    |            561 |                               3 |       0.686456 |              0.691633 |                     0.00517696 | 0.0348714 |        0.0334325 |               -0.00143891 |
| moneyline | final_test_reference |            157 |                             757 |       0.65888  |              0.661909 |                     0.00302922 | 0.0604997 |        0.0666541 |                0.00615442 |
| run_line  | cv_fold_2            |            448 |                             392 |       0.636507 |              0.634916 |                    -0.00159124 | 0.0638297 |        0.0535021 |               -0.0103276  |
| run_line  | cv_fold_3            |            384 |                             840 |       0.65444  |              0.647606 |                    -0.00683374 | 0.0482442 |        0.0184488 |               -0.0297955  |
| run_line  | cv_fold_4            |            290 |                            1224 |       0.660766 |              0.656813 |                    -0.00395341 | 0.068263  |        0.046358  |               -0.021905   |
| run_line  | crossfit_combined    |           1122 |                               3 |       0.648915 |              0.644919 |                    -0.00399601 | 0.053189  |        0.0141284 |               -0.0390606  |
| run_line  | final_test_reference |            314 |                            1514 |       0.65191  |              0.643432 |                    -0.00847752 | 0.0721721 |        0.0483821 |               -0.02379    |
| total     | cv_fold_2            |           1344 |                            1176 |       0.660471 |              0.64949  |                    -0.0109807  | 0.0730854 |        0.0329618 |               -0.0401236  |
| total     | cv_fold_3            |           1152 |                            2520 |       0.675636 |              0.653843 |                    -0.0217931  | 0.104934  |        0.0708756 |               -0.0340587  |
| total     | cv_fold_4            |            870 |                            3672 |       0.684096 |              0.641488 |                    -0.042608   | 0.165211  |        0.119005  |               -0.0462066  |
| total     | crossfit_combined    |           3366 |                               3 |       0.671767 |              0.648911 |                    -0.0228558  | 0.100889  |        0.062981  |               -0.0379078  |
| total     | final_test_reference |            942 |                            4542 |       0.665757 |              0.651888 |                    -0.0138686  | 0.0749247 |        0.0377767 |               -0.0371481  |

## Production calibrators

### moneyline

- method: `beta_logistic`
- enabled: `False`
- fit observations: `757`
- fit periods: `cv_fold_1, cv_fold_2, cv_fold_3, cv_fold_4`
- a: `0.030982877797`
- b: `1.422230447719`
- intercept: `-0.910880149011`

### run_line

- method: `beta_logistic`
- enabled: `True`
- fit observations: `1514`
- fit periods: `cv_fold_1, cv_fold_2, cv_fold_3, cv_fold_4`
- a: `0.807480711187`
- b: `0.663717974928`
- intercept: `0.071162137965`

### total

- method: `beta_logistic`
- enabled: `True`
- fit observations: `4542`
- fit periods: `cv_fold_1, cv_fold_2, cv_fold_3, cv_fold_4`
- a: `0.466631572866`
- b: `0.808146196172`
- intercept: `-0.533222228900`

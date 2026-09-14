# Moneyline Extra-Innings Backtest

## Question

Does the current method of conditioning away modeled regulation-score ties
produce better final-game MLB moneyline probabilities than explicitly
allocating tie mass to an extra-inning outcome model?

## Methods

- `current_conditioned`: remove Skellam tie mass and renormalize decisive outcomes.
- `explicit_extra_50_50`: allocate 50% of tie mass to the home team.
- `explicit_extra_empirical`: allocate tie mass using the strictly prior
  Beta-smoothed empirical home win rate among games actually tied after nine.

Lower log loss, ECE, and Brier score are better. For the explicit methods,
`log_loss_delta_vs_current < 0` means improvement over current production.

## Metrics

| period              | method                   |   games |   moneyline_log_loss |   moneyline_ece |   moneyline_brier |   log_loss_delta_vs_current |   log_loss_delta_bootstrap_95_low |   log_loss_delta_bootstrap_95_high |   mean_home_probability |   mean_absolute_probability_change_vs_current |   mean_modeled_regulation_tie_probability |
|:--------------------|:-------------------------|--------:|---------------------:|----------------:|------------------:|----------------------------:|----------------------------------:|-----------------------------------:|------------------------:|----------------------------------------------:|------------------------------------------:|
| cv_fold_1           | current_conditioned      |     196 |             0.684819 |       0.0892754 |          0.245944 |                 0           |                        0          |                        0           |                0.497616 |                                    0          |                                  0.132954 |
| cv_fold_1           | explicit_extra_50_50     |     196 |             0.683511 |       0.0733866 |          0.245319 |                -0.00130833  |                       -0.00516471 |                        0.00235443  |                0.497907 |                                    0.0100414  |                                  0.132954 |
| cv_fold_1           | explicit_extra_empirical |     196 |             0.682394 |       0.0720492 |          0.24477  |                -0.00242574  |                       -0.00666888 |                        0.00161889  |                0.503382 |                                    0.0108814  |                                  0.132954 |
| cv_fold_2           | current_conditioned      |     224 |             0.673354 |       0.0393776 |          0.240287 |                 0           |                        0          |                        0           |                0.494904 |                                    0          |                                  0.131811 |
| cv_fold_2           | explicit_extra_50_50     |     224 |             0.674323 |       0.0401081 |          0.240733 |                 0.000968677 |                       -0.00190126 |                        0.00377256  |                0.495635 |                                    0.00873757 |                                  0.131811 |
| cv_fold_2           | explicit_extra_empirical |     224 |             0.674542 |       0.0453602 |          0.240839 |                 0.0011877   |                       -0.00176574 |                        0.00407771  |                0.497674 |                                    0.00902264 |                                  0.131811 |
| cv_fold_3           | current_conditioned      |     192 |             0.695194 |       0.0310675 |          0.250971 |                 0           |                        0          |                        0           |                0.500977 |                                    0          |                                  0.129848 |
| cv_fold_3           | explicit_extra_50_50     |     192 |             0.693284 |       0.0192337 |          0.250055 |                -0.00191037  |                       -0.00517302 |                        0.00120454  |                0.500874 |                                    0.00896992 |                                  0.129848 |
| cv_fold_3           | explicit_extra_empirical |     192 |             0.693291 |       0.0192452 |          0.250058 |                -0.0019025   |                       -0.00511935 |                        0.00122914  |                0.501439 |                                    0.00895872 |                                  0.129848 |
| cv_fold_4           | current_conditioned      |     145 |             0.695127 |       0.0953428 |          0.250556 |                 0           |                        0          |                        0           |                0.493561 |                                    0          |                                  0.130515 |
| cv_fold_4           | explicit_extra_50_50     |     145 |             0.692391 |       0.082918  |          0.249383 |                -0.00273559  |                       -0.00742322 |                        0.00170212  |                0.494511 |                                    0.0102244  |                                  0.130515 |
| cv_fold_4           | explicit_extra_empirical |     145 |             0.692546 |       0.0831031 |          0.249455 |                -0.0025805   |                       -0.0072359  |                        0.001866    |                0.493997 |                                    0.0101979  |                                  0.130515 |
| final_test          | current_conditioned      |     157 |             0.65888  |       0.0604997 |          0.233617 |                 0           |                        0          |                        0           |                0.513291 |                                    0          |                                  0.132095 |
| final_test          | explicit_extra_50_50     |     157 |             0.66034  |       0.057126  |          0.234177 |                 0.00145965  |                       -0.00307656 |                        0.00569576  |                0.51174  |                                    0.0115386  |                                  0.132095 |
| final_test          | explicit_extra_empirical |     157 |             0.66034  |       0.0642725 |          0.234162 |                 0.00145962  |                       -0.00323183 |                        0.00595306  |                0.508461 |                                    0.0122317  |                                  0.132095 |
| validation_combined | current_conditioned      |     757 |             0.686032 |       0.0251081 |          0.246428 |                 0           |                        0          |                        0           |                0.496889 |                                    0          |                                  0.131361 |
| validation_combined | explicit_extra_50_50     |     757 |             0.684972 |       0.0191756 |          0.245942 |                -0.00106063  |                       -0.00285503 |                        0.000686336 |                0.497337 |                                    0.00941887 |                                  0.131361 |
| validation_combined | explicit_extra_empirical |     757 |             0.684779 |       0.0212684 |          0.245845 |                -0.00125344  |                       -0.00310984 |                        0.000561724 |                0.499402 |                                    0.0097128  |                                  0.131361 |

## Chronological extra-inning parameters

| period     | eval_start   | eval_end   |   history_games |   prior_extra_games |   prior_extra_home_wins |   q_extra_home |   evaluation_games |   evaluation_extra_games |   evaluation_extra_home_win_rate |   evaluation_extra_frequency |
|:-----------|:-------------|:-----------|----------------:|--------------------:|------------------------:|---------------:|-------------------:|-------------------------:|---------------------------------:|-----------------------------:|
| cv_fold_1  | 2026-06-03   | 2026-06-20 |             909 |                  83 |                      45 |       0.541176 |                196 |                       12 |                         0.333333 |                    0.0612245 |
| cv_fold_2  | 2026-06-21   | 2026-07-07 |            1105 |                  95 |                      49 |       0.515464 |                224 |                       18 |                         0.444444 |                    0.0803571 |
| cv_fold_3  | 2026-07-08   | 2026-07-27 |            1329 |                 113 |                      57 |       0.504348 |                192 |                       12 |                         0.416667 |                    0.0625    |
| cv_fold_4  | 2026-07-28   | 2026-08-13 |            1521 |                 125 |                      62 |       0.496063 |                145 |                       14 |                         0.285714 |                    0.0965517 |
| final_test | 2026-08-14   | 2026-09-10 |            1666 |                 139 |                      66 |       0.475177 |                157 |                       14 |                         0.357143 |                    0.089172  |

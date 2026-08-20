# MLB Pipeline Rebuild TODO
## SportsDataverse Integration, Probability Repair, EV/Kelly Repair, and Run-Line Selection

**Repository scope:** `docs/win/baseball/mlb`  
**Basis:** current GitHub `main` structure reviewed on 2026-08-20.  
**Important constraint:** this is an implementation plan only. No GitHub files were modified while preparing it.

---

## Objective

Rebuild the MLB prediction path so that:

1. **Sportsbook prices do not alter model probabilities.**
2. **SportsDataverse information changes the baseball projection itself**, rather than directly nudging an already-calculated EV.
3. **Home and away expected runs become the common model foundation** for moneyline, run line, and totals.
4. **EV and Kelly are calculated from the exact same probability.**
5. **Whole-number totals preserve push probability.**
6. **Both `+1.5` and `-1.5` run-line sides are evaluated on value**, instead of one side being structurally favored by probability ranking or asymmetric eligibility.
7. **Training data is time-safe**: every feature used for a historical game must contain only information available before that game.
8. **Existing DRatings projections are preserved as baseline model inputs**, not discarded.
9. **SportsDataverse columns do not need to be copied through every downstream CSV.** The run-projection stage consumes SDV features and produces compact model outputs for the existing pipeline.

The target production flow is:

```text
Sportsbook odds
    |
    | used only as the offered price
    v
DRatings prediction
+ SportsDataverse pregame features
+ pregame weather / park information
    |
    v
build_run_projection.py
    |
    +--> model_home_runs
    +--> model_away_runs
    +--> model_total_runs
    |
    v
build_juice_files.py
(repurposed as market-probability builder)
    |
    +--> moneyline model probabilities
    +--> run-line model probabilities
    +--> total win/loss/push probabilities
    |
    v
compute_edges.py
(probability edge only)
    |
    v
compute_ev_kelly.py
(EV and Kelly from identical probabilities)
    |
    v
baseball_select_bets.py / baseball_select_bets_AM.py
(value-based candidate comparison)
```

---

# TODO 1 — Define one canonical probability contract

### Files affected

- `docs/win/baseball/mlb/scripts/01_merge/build_juice_files.py`
- `docs/win/baseball/mlb/scripts/03_edges/compute_edges.py`
- `docs/win/baseball/mlb/scripts/03_edges/compute_ev_kelly.py`
- `docs/win/baseball/mlb/scripts/04_select/baseball_select_bets.py`
- `docs/win/baseball/mlb/scripts/04_select/baseball_select_bets_AM.py`

### Exact change

Stop using the current `*_normalized_prob_*` columns as the official model-probability interface. Those columns currently represent probabilities after the separate juice scripts have changed them.

Introduce these canonical model columns:

```text
home_model_prob_moneyline
away_model_prob_moneyline

home_model_prob_run_line
away_model_prob_run_line

over_model_prob_total_win
over_model_prob_total_loss
under_model_prob_total_win
under_model_prob_total_loss
total_model_prob_push
```

Also establish these run-projection columns:

```text
dratings_home_projected_runs
dratings_away_projected_runs
dratings_total_projected_runs

model_home_runs
model_away_runs
model_total_runs
```

### Required behavior

For moneyline:

```text
home_model_prob_moneyline + away_model_prob_moneyline = 1
```

For a standard `-1.5/+1.5` run-line market:

```text
home_model_prob_run_line + away_model_prob_run_line = 1
```

For totals:

```text
over_model_prob_total_win
+ over_model_prob_total_loss
+ total_model_prob_push
= 1
```

and:

```text
under_model_prob_total_win  = over_model_prob_total_loss
under_model_prob_total_loss = over_model_prob_total_win
```

### Migration rule

Update every downstream consumer to read the new canonical columns before removing the old `*_normalized_prob_*` columns. Do not silently reuse an old column name for a new meaning.

### Validation

Every output file must fail validation if:

- a required canonical probability is missing;
- any probability is below `0` or above `1`;
- a moneyline pair does not sum to `1` within `1e-6`;
- a run-line pair does not sum to `1` within `1e-6`;
- a totals win/loss/push triplet does not sum to `1` within `1e-6`.

---

# TODO 2 — Remove sportsbook-price adjustments from the model-probability path

### Current files involved

- `docs/win/baseball/mlb/scripts/02_juice/apply_moneyline_juice.py`
- `docs/win/baseball/mlb/scripts/02_juice/apply_run_line_juice.py`
- `docs/win/baseball/mlb/scripts/02_juice/apply_total_juice.py`
- `docs/win/baseball/mlb/config/juice/mlb_ml_juice.csv`
- `docs/win/baseball/mlb/config/juice/mlb_run_line_juice.csv`
- `docs/win/baseball/mlb/config/juice/mlb_totals_juice.csv`
- `.github/workflows/mlb_01_morning.yml`

### Exact change

The three `apply_*_juice.py` scripts must no longer sit between model probability generation and EV/Kelly.

Remove these workflow steps from `mlb_01_morning.yml`:

```text
MLB Moneyline Juice
MLB Run Line Juice
MLB Total Juice
```

Do not delete the scripts or CSV configurations immediately. Leave them in the repository during migration so old output can still be inspected, but they must not execute in the production probability path.

### `build_juice_files.py` output location

Change:

```python
OUTPUT_DIR = INPUT_DIR / "01_merguiced"
```

to the existing downstream directory:

```text
docs/win/baseball/mlb/02_juice
```

This lets `build_juice_files.py` write the completed market-probability files directly where `compute_edges.py` already expects them.

### Sportsbook odds rule

Sportsbook odds may be used only for:

- `dk_*_american`
- `dk_*_decimal`
- break-even probability
- probability edge versus the offered price
- EV
- Kelly
- final candidate ranking / bet selection

Sportsbook odds must not be used to:

- add or subtract probability from the model;
- select a probability calibration band;
- renormalize a model probability;
- alter `model_home_runs` or `model_away_runs`.

### Validation

Add a log line in the market-probability stage:

```text
MODEL PROBABILITIES ARE PRICE-INDEPENDENT: sportsbook odds are not probability inputs
```

The probability-building functions should accept model runs / model probability inputs and market lines. They should not accept DK odds as probability-calculation arguments.

---

# TODO 3 — Rebuild `build_juice_files.py` as the market-probability builder

The filename can remain unchanged initially to avoid unnecessary path churn. Its behavior needs to change.

### File

`docs/win/baseball/mlb/scripts/01_merge/build_juice_files.py`

### Input

Continue reading the three files created by `merge_intake.py`:

```text
01_merge/{date}_mlb_moneyline.csv
01_merge/{date}_mlb_run_line.csv
01_merge/{date}_mlb_total.csv
```

After TODO 9 is completed, these files must contain:

```text
model_home_runs
model_away_runs
model_total_runs
```

and the original DRatings values must remain available under their explicit `dratings_*` names.

---

## 3A — Moneyline probability

Stop treating DRatings `home_prob` / `away_prob` as the final production probability once the new run model is active.

Use the difference distribution generated by:

```text
Home runs ~ Poisson(model_home_runs)
Away runs ~ Poisson(model_away_runs)
```

Use the Skellam distribution for:

```text
P(home runs > away runs)
P(away runs > home runs)
P(tie after the modeled scoring period)
```

Because a normal completed MLB game ultimately has a winner, convert the regulation scoring distribution to a two-sided moneyline probability by conditioning on a non-tie:

```text
home_model_prob_moneyline =
    P(home > away) / [P(home > away) + P(away > home)]

away_model_prob_moneyline =
    P(away > home) / [P(home > away) + P(away > home)]
```

Write both canonical columns.

Also write audit columns:

```text
model_prob_moneyline_tie_raw
home_model_fair_decimal_moneyline
away_model_fair_decimal_moneyline
```

with:

```text
fair_decimal = 1 / model_probability
```

The raw tie probability is retained only for audit; it is not a sportsbook push probability.

---

## 3B — Run-line probability

Use:

```text
model_home_runs
model_away_runs
home_run_line
away_run_line
```

For the standard `-1.5/+1.5` pair, calculate cover probability directly from the Skellam run-difference distribution.

Write:

```text
home_model_prob_run_line
away_model_prob_run_line

home_model_fair_decimal_run_line
away_model_fair_decimal_run_line
```

Do not infer favorite/underdog from the sportsbook price to calculate these probabilities.

Do not clamp normal probabilities to `0.01` or `0.99` unless the underlying numerical library returns an invalid boundary value. A valid model probability should remain the actual calculated value.

Hard-fail a row if:

- the two run lines are missing;
- the market is not a complementary pair;
- the current implementation receives a line type it does not support.

Do not silently approximate an unsupported line.

---

## 3C — Totals probability

Use:

```text
lambda_total = model_home_runs + model_away_runs
```

For a half-run total such as `8.5`:

```text
p_push = 0
p_under = P(total runs <= 8)
p_over  = P(total runs >= 9)
```

For a whole-run total such as `8.0`:

```text
p_under = P(total runs <= 7)
p_push  = P(total runs = 8)
p_over  = P(total runs >= 9)
```

Write:

```text
over_model_prob_total_win
over_model_prob_total_loss

under_model_prob_total_win
under_model_prob_total_loss

total_model_prob_push
```

Set:

```text
over_model_prob_total_loss  = p_under
under_model_prob_total_loss = p_over
```

### Fair decimal for totals

For a bet with a possible push, fair decimal is not simply `1 / p_win`.

Use:

```text
fair_decimal = 1 + (p_loss / p_win)
```

Therefore write:

```text
fair_total_over_decimal =
    1 + over_model_prob_total_loss / over_model_prob_total_win

fair_total_under_decimal =
    1 + under_model_prob_total_loss / under_model_prob_total_win
```

For half-run totals, this reduces to the normal `1 / p_win`.

---

# TODO 4 — Make `compute_edges.py` calculate probability edge only

### File

`docs/win/baseball/mlb/scripts/03_edges/compute_edges.py`

### Remove from this file

Remove the contextual EV/edge modification system:

- `edge_adjustments.yaml` loading;
- starting-pitcher xwOBA edge nudges;
- lineup xwOBA edge nudges;
- park-factor edge nudges;
- weather edge multipliers;
- any function whose purpose is to modify an already-calculated edge because of baseball context.

SportsDataverse, pitcher quality, lineup information, park conditions, and weather belong upstream in the run projection.

### Redefine edge

For moneyline:

```text
home_market_break_even_prob = 1 / home_dk_decimal_moneyline
away_market_break_even_prob = 1 / away_dk_decimal_moneyline

home_edge_prob_moneyline =
    home_model_prob_moneyline - home_market_break_even_prob

away_edge_prob_moneyline =
    away_model_prob_moneyline - away_market_break_even_prob
```

For run line:

```text
home_market_break_even_prob_run_line = 1 / home_dk_run_line_decimal
away_market_break_even_prob_run_line = 1 / away_dk_run_line_decimal

home_edge_prob_run_line =
    home_model_prob_run_line - home_market_break_even_prob_run_line

away_edge_prob_run_line =
    away_model_prob_run_line - away_market_break_even_prob_run_line
```

For totals with a push, compare conditional win probability on resolved bets:

```text
over_conditional_win_prob =
    over_model_prob_total_win /
    (over_model_prob_total_win + over_model_prob_total_loss)

under_conditional_win_prob =
    under_model_prob_total_win /
    (under_model_prob_total_win + under_model_prob_total_loss)
```

Then:

```text
over_edge_prob_total =
    over_conditional_win_prob - (1 / dk_total_over_decimal)

under_edge_prob_total =
    under_conditional_win_prob - (1 / dk_total_under_decimal)
```

### Result

After this change:

- **edge** means probability advantage over the offered break-even price;
- **EV** means expected profit per unit staked;
- those are no longer two names for the same calculation.

---

# TODO 5 — Rebuild `compute_ev_kelly.py` so EV and Kelly share one probability basis

### File

`docs/win/baseball/mlb/scripts/03_edges/compute_ev_kelly.py`

### Remove

Remove the current concept of:

```text
raw EV
adjusted EV
final EV copied from adjusted edge
```

Remove the helper path where contextual edge is substituted for EV.

There should be no circumstance where context changes EV without first changing the model probability.

---

## 5A — Moneyline EV/Kelly

For each side:

```text
p = canonical model probability
d = sportsbook decimal odds
b = d - 1
q = 1 - p
```

Calculate:

```text
EV = p * d - 1
```

Calculate full Kelly:

```text
Kelly = ((b * p) - q) / b
```

Clip Kelly below zero to zero only after the raw value has been calculated and audited.

Write:

```text
home_ml_ev
away_ml_ev

home_ml_kelly
away_ml_kelly
```

---

## 5B — Run-line EV/Kelly

Use the identical formula, with:

```text
p = home_model_prob_run_line / away_model_prob_run_line
d = corresponding DK run-line decimal price
```

Write:

```text
home_rl_ev
away_rl_ev

home_rl_kelly
away_rl_kelly
```

---

## 5C — Totals EV/Kelly with pushes

For each total side:

```text
p_win
p_loss
p_push
d
b = d - 1
```

Calculate:

```text
EV = p_win * b - p_loss
```

Do not charge a loss for `p_push`.

Calculate Kelly:

```text
Kelly =
    (p_win * b - p_loss)
    /
    (b * (p_win + p_loss))
```

If:

```text
p_win + p_loss = 0
```

the row is invalid.

Write:

```text
over_ev
under_ev

over_kelly
under_kelly
```

---

## 5D — Add hard mathematical consistency checks

After EV and Kelly are calculated, validate every side.

Within a small numeric tolerance:

```text
EV > 0  => raw Kelly > 0
EV < 0  => raw Kelly < 0
EV = 0  => raw Kelly = 0
```

A sign mismatch should be an error, not a warning.

The current category of:

```text
positive EV but zero Kelly
raw EV / adjusted EV sign flip
adjusted-only positive
```

should disappear because there is no longer a second probability/EV system.

Keep audit columns showing:

```text
prob_for_ev
prob_for_kelly
ev_probability_source
kelly_probability_source
```

but both sources must point to the same canonical model probability column.

---

# TODO 6 — Preserve the current SportsDataverse output as a separate model input

### Existing file

`docs/win/baseball/mlb/scripts/00_intake/sportsdataverse_mlb.py`

### Existing output

```text
docs/win/baseball/mlb/00_intake/sportsdataverse/{date}_sportsdataverse.csv
```

### Keep

Keep the current pregame cutoff rule:

```text
sdv_as_of_date = game_date - 1 day
```

Same-day pitches must remain excluded.

Keep the existing starter fields, including:

```text
sdv_home_sp_xera
sdv_away_sp_xera

sdv_home_sp_xera_30d
sdv_away_sp_xera_30d

sdv_home_sp_xwoba
sdv_away_sp_xwoba

sdv_home_sp_xwoba_30d
sdv_away_sp_xwoba_30d

sdv_home_sp_stuff_plus
sdv_away_sp_stuff_plus

sdv_home_sp_command_plus
sdv_away_sp_command_plus

sdv_home_sp_avg_velo
sdv_away_sp_avg_velo

sdv_home_sp_avg_velo_30d
sdv_away_sp_avg_velo_30d

sdv_home_sp_velo_delta_30d
sdv_away_sp_velo_delta_30d

sdv_home_sp_pitches
sdv_away_sp_pitches

sdv_home_sp_pitches_30d
sdv_away_sp_pitches_30d
```

For model-facing names, document:

```text
sdv_*_sp_stuff_plus   = SportsDataverse pitch-quality score
sdv_*_sp_command_plus = SportsDataverse location/command-quality score
```

Do not rename the raw SDV columns unless the source script is changed consistently. The model-training code can map them to clearer feature labels internally.

### Do NOT do

Do not add all `sdv_*` columns to `enrich_game_context.py`.

Do not copy all `sdv_*` columns through:

```text
merge_intake.py
build_juice_files.py
compute_edges.py
compute_ev_kelly.py
```

The new run-projection stage will read the SDV file directly and convert those features into `model_home_runs` and `model_away_runs`.

This keeps SDV as a model input instead of turning it into uncontrolled downstream metadata.

---

# TODO 7 — Build leakage-safe historical SportsDataverse data for model training

The current SDV script produces the latest date by default. Training requires the same feature definitions for historical dates.

### File to extend

`docs/win/baseball/mlb/scripts/00_intake/sportsdataverse_mlb.py`

### Add CLI support

Add:

```text
--from-date YYYY-MM-DD
--to-date YYYY-MM-DD
```

Behavior:

- if positional dates are provided, continue supporting them;
- if `--from-date` and `--to-date` are provided, process every game date in that inclusive range for which `00_intake/games/{date}_games.csv` exists;
- do not process dates with no games file.

### Add raw Statcast cache

Add directory:

```text
docs/win/baseball/mlb/00_intake/sportsdataverse/cache/
```

Cache raw pitcher Statcast data by season:

```text
{season}_pitcher_statcast.parquet
```

The cache may contain pitches from the full downloaded season, but **feature construction for a historical game must filter the cached rows before aggregation**:

```text
pitch.game_date < target_game_date
```

For the 30-day values:

```text
target_game_date - 30 days <= pitch.game_date < target_game_date
```

The cutoff filter must happen before:

- xERA calculation;
- xwOBA aggregation;
- pitch-quality score aggregation;
- command/location score aggregation;
- velocity aggregation;
- spin aggregation;
- pitch counts;
- game counts.

### Why this is required

Historical feature rows must reproduce what could have been known before each game. A full-season aggregate cannot be used as a feature for an April or May game.

### Output rule

Each historical date continues to get its own:

```text
{date}_sportsdataverse.csv
```

with:

```text
sdv_as_of_date
sdv_status
sdv_home_sp_found
sdv_away_sp_found
```

### Validation

For every historical output row:

```text
sdv_as_of_date < game_date
```

Any violation is a fatal error.

---

# TODO 8 — Do not train on current historical `game_context` season aggregates unless they are rebuilt as-of

### Existing behavior to account for

`enrich_game_context.py` reads annual cleaned batting and pitching files and uses those player-level rows when producing game context.

That is acceptable as a current-day context snapshot, but it is not automatically safe as historical model-training data because the annual file may contain information accumulated after the historical game date.

### Exact training rule

For the first production run model, **exclude these existing historical context statistics from the training feature matrix**:

```text
home_sp_xwoba
away_sp_xwoba
home_sp_k_pct
away_sp_k_pct
home_sp_bb_pct
away_sp_bb_pct
home_sp_barrel_pct
away_sp_barrel_pct
home_sp_whiff_pct
away_sp_whiff_pct

home_lineup_xwoba
away_lineup_xwoba
home_lineup_barrel_pct
away_lineup_barrel_pct
home_lineup_hard_hit_pct
away_lineup_hard_hit_pct
home_lineup_k_pct
away_lineup_k_pct
home_lineup_bb_pct
away_lineup_bb_pct
home_lineup_exit_velo
away_lineup_exit_velo
```

Do not use those historical columns until a separate as-of-date reconstruction exists.

### Safe initial model inputs

Use inputs that can be tied to a pregame date:

```text
DRatings projected runs
DRatings win probability
SportsDataverse starter features with day-before cutoff
pregame weather values
venue / roof / day-night identifiers if encoded safely
```

Park factors may be added only if the stored factor can be shown to represent information available at the prediction date. Otherwise leave them out of the first trained model and add them after the as-of provenance is established.

### Later lineup work

If lineup performance is added to the trained model, build a separate as-of batter feature process from historical lineup MLBAM IDs and pregame-only Statcast rows. Do not use a season-end batting table as a shortcut.

---

# TODO 9 — Create the leakage-safe run-model training dataset

### New script

Create:

```text
docs/win/baseball/mlb/scripts/modeling/build_run_training_set.py
```

### New output directory

Create:

```text
docs/win/baseball/mlb/modeling/data/
```

### Output file

```text
docs/win/baseball/mlb/modeling/data/mlb_run_training_set.csv
```

### Required source files per date

Read:

```text
00_intake/predictions/pred_with_game_id/{date}_MLB.csv
00_intake/games/{date}_games.csv
00_intake/sportsdataverse/{date}_sportsdataverse.csv
05_final_scores/results/final_scores/{date}_final_scores_MLB.csv
```

Weather may be joined from:

```text
data/weather/{date}_weather.csv
```

provided the weather row was generated pregame and is keyed to the same `gamePk`.

### Join keys

Use:

```text
prediction.game_id -> games.game_id
games.gamePk -> sportsdataverse.gamePk
games.gamePk -> final_scores.gamePk
```

Use `game_id` as a secondary consistency check where it exists in both files.

Hard-fail duplicate `game_id` or duplicate `gamePk` within any source for the same date.

### Feature columns

Preserve DRatings source values explicitly:

```text
dratings_home_prob
dratings_away_prob
dratings_home_projected_runs
dratings_away_projected_runs
dratings_total_projected_runs
```

Map the SportsDataverse starter inputs into model-facing features:

```text
home_sp_pitch_quality_plus
away_sp_pitch_quality_plus

home_sp_command_plus
away_sp_command_plus

home_sp_xera
away_sp_xera

home_sp_xera_30d
away_sp_xera_30d

home_sp_xwoba
away_sp_xwoba

home_sp_xwoba_30d
away_sp_xwoba_30d

home_sp_avg_velo
away_sp_avg_velo

home_sp_avg_velo_30d
away_sp_avg_velo_30d

home_sp_velo_delta_30d
away_sp_velo_delta_30d

home_sp_pitches
away_sp_pitches

home_sp_games
away_sp_games

home_sp_pitches_30d
away_sp_pitches_30d

home_sp_games_30d
away_sp_games_30d
```

The mapping is:

```text
*_pitch_quality_plus <- sdv_*_sp_stuff_plus
*_command_plus       <- sdv_*_sp_command_plus
```

Include pregame environment fields only when their provenance is safe:

```text
temp_f
wind_mph
wind_blowing_out
humidity
air_pressure_at_sea_level
dew_point_f
weather_applicable
```

### Forbidden training inputs

Do not include:

```text
DK moneyline odds
DK run-line odds
DK total odds
market implied probability
juice-band values
current/final game score-derived fields other than the target
graded bet results
win/loss labels from selected bets
```

The sportsbook price is not a feature in the baseball run model.

### Targets

From final scores:

```text
target_home_runs = final_home_score
target_away_runs = final_away_score
```

Only include rows with:

```text
game_status = final
```

and valid numeric final scores.

### Required audit columns

Keep these in the training CSV but exclude them from the model matrix:

```text
game_date
game_id
gamePk
home_team
away_team
sdv_as_of_date
```

### Validation

Reject a training row if:

```text
sdv_as_of_date >= game_date
```

Log counts for:

```text
rows_loaded
rows_joined
missing_sdv
missing_final_score
duplicate_game_id
duplicate_gamePk
leakage_rejections
rows_written
```

---

# TODO 10 — Train and save the home-run and away-run models

### New dependency

Add to root `requirements.txt`:

```text
scikit-learn
```

`joblib` is installed as a scikit-learn dependency and can be used for model persistence.

### New script

Create:

```text
docs/win/baseball/mlb/scripts/modeling/train_run_model.py
```

### New model directory

Create:

```text
docs/win/baseball/mlb/models/run_projection/
```

### Models

Train two separate regressors:

```text
home_runs_model.joblib
away_runs_model.joblib
```

Use `HistGradientBoostingRegressor` with Poisson loss because:

- run targets are non-negative;
- the model can learn nonlinear relationships;
- it accepts missing numeric values;
- it avoids forcing hand-written coefficients for SDV signals.

### Chronological split

Sort completed games by `game_date`.

Use unique game dates, not randomly shuffled rows.

Split chronologically:

```text
first 70% of dates  -> training
next 15% of dates   -> validation
final 15% of dates  -> untouched test
```

Games from the same date must remain in the same split.

### Hyperparameter selection

Use the training and validation portions only.

Search:

```text
learning_rate:       0.03, 0.05, 0.10
max_leaf_nodes:      7, 15, 31
min_samples_leaf:    10, 20, 40
l2_regularization:   0, 1, 5
```

Select the configuration with the lowest validation mean Poisson deviance.

Do this separately for the home-run and away-run models.

Do not choose parameters using the final test portion.

### Baseline comparison

The test report must compare:

```text
new home-run prediction
vs
dratings_home_projected_runs

new away-run prediction
vs
dratings_away_projected_runs
```

Report at minimum:

```text
MAE
mean Poisson deviance
mean predicted runs
mean actual runs
```

### Model metadata

Save:

```text
home_runs_model_metadata.json
away_runs_model_metadata.json
```

Each metadata file must contain:

```text
training_start_date
training_end_date
validation_start_date
validation_end_date
test_start_date
test_end_date
feature_columns
selected_hyperparameters
training_row_count
validation_row_count
test_row_count
baseline_metrics
model_metrics
created_at
```

### Production artifact rule

The prediction script must read the exact feature list from model metadata. It must not silently accept a different feature order.

---

# TODO 11 — Create the production run-projection stage

### New script

Create:

```text
docs/win/baseball/mlb/scripts/00_intake/build_run_projection.py
```

### Inputs

For each date:

```text
00_intake/predictions/pred_with_game_id/{date}_MLB.csv
00_intake/games/{date}_games.csv
00_intake/sportsdataverse/{date}_sportsdataverse.csv
00_intake/mlb_raw/{date}_game_context.csv
models/run_projection/home_runs_model.joblib
models/run_projection/away_runs_model.joblib
models/run_projection/home_runs_model_metadata.json
models/run_projection/away_runs_model_metadata.json
```

`game_context` is used only for model features that were explicitly included during training and have safe provenance, such as pregame weather. Do not feed every context column automatically.

### Output directory

Create:

```text
docs/win/baseball/mlb/00_intake/predictions/model_projection/
```

### Output file

```text
{date}_MLB.csv
```

### Output row structure

Start from the existing prediction row.

Preserve original DRatings values by writing:

```text
dratings_home_prob
dratings_away_prob
dratings_home_projected_runs
dratings_away_projected_runs
dratings_total_projected_runs
```

Then add:

```text
model_home_runs
model_away_runs
model_total_runs
run_model_version
run_model_feature_status
```

Calculate:

```text
model_total_runs = model_home_runs + model_away_runs
```

### Do not overwrite source data

Do not overwrite:

```text
pred_with_game_id/{date}_MLB.csv
```

The new model output is a separate file so DRatings can always be compared with the rebuilt projection.

### Missing SDV data

Do not substitute arbitrary league-average values in this script.

Because the chosen model supports missing numeric values, missing SDV values may remain `NaN` if:

- the model was trained with the same missing-value behavior;
- sample-count/found flags are available;
- the prediction row logs which features were missing.

Write:

```text
run_model_feature_status
```

with an auditable status indicating whether both probable starters had SDV data.

### Prediction validation

Hard-fail if:

- `game_id` is blank;
- duplicate `game_id` exists;
- the SDV file contains duplicate `gamePk`;
- the model feature order differs from metadata;
- a predicted run value is non-finite;
- a predicted run value is negative.

Do not silently clip a broken negative prediction. Treat it as a model/output error.

---

# TODO 12 — Change `merge_intake.py` to consume the model-projection file

### File

`docs/win/baseball/mlb/scripts/01_merge/merge_intake.py`

### Change prediction input directory

Change:

```text
00_intake/predictions/pred_with_game_id
```

to:

```text
00_intake/predictions/model_projection
```

### Update `REQUIRED_PRED_COLS`

Keep the existing identity/team/pitcher fields.

Require:

```text
dratings_home_prob
dratings_away_prob
dratings_home_projected_runs
dratings_away_projected_runs
dratings_total_projected_runs

model_home_runs
model_away_runs
model_total_runs

run_model_version
run_model_feature_status
```

### Market outputs

Write the new projection columns into all three merged market files because all three market-probability calculations require the same run distribution.

Do not copy the full SportsDataverse feature file into the merge.

The merged market files need the model result, not every raw feature that produced it.

### Existing context

Keep the current `CONTEXT_COLS` output for audit and selection context if still required elsewhere.

However, downstream EV math must no longer read those columns to modify EV.

### Reconciliation

Add model projection to the existing source audit:

```text
source_present_model_projection
```

A sportsbook/prediction game that has no model projection should be a hard failure for production output.

---

# TODO 13 — Make run-line selection value-based and side-neutral

### Files

- `docs/win/baseball/mlb/scripts/04_select/baseball_select_bets.py`
- `docs/win/baseball/mlb/scripts/04_select/baseball_select_bets_AM.py`
- `docs/win/baseball/mlb/config/markets.yaml`
- `docs/win/baseball/mlb/config/markets_AM.yaml`

### Probability used by the candidate

For run line:

```text
home candidate model_prob = home_model_prob_run_line
away candidate model_prob = away_model_prob_run_line
```

The same probability must already have been used for:

```text
EV
Kelly
selection audit
```

### Candidate ranking

For a game where both run-line sides are valid candidates:

```text
select the candidate with the larger EV
```

Do not rank the two sides by highest probability.

Set in both market configuration files:

```yaml
run_line:
  pick_preference: best_ev
```

### Make both line sides eligible for evaluation

During the rebuilt-model validation period, use the same valid market domain for home and away run-line candidates.

Set both home and away run-line sections to accept:

```yaml
line_bands:
  - [-1.5, -1.5]
  - [1.5, 1.5]

odds_bands:
  - [-1000, 1000]

prob_bands:
  - [0.0, 1.0]
```

EV must be positive and Kelly must be positive in code before a candidate can be selected.

The purpose is to make `+1.5` and `-1.5` compete using the model probability and the actual offered price, not to preselect one price type.

### Selection audit

For every run-line game, write an audit row for both sides before the final one-side choice.

Required audit fields:

```text
game_id
side
line
model_probability
dk_decimal
break_even_probability
probability_edge
ev
kelly
candidate_passed
candidate_rejection_reason
selected
```

This makes it possible to answer exactly why `+1.5` or `-1.5` won the comparison for any game.

---

# TODO 14 — Update the morning workflow in the correct order

### File

`.github/workflows/mlb_01_morning.yml`

### Add SportsDataverse feature generation

Immediately after:

```text
MLB Build Games List
```

add:

```text
MLB SportsDataverse Features
```

running:

```text
python docs/win/baseball/mlb/scripts/00_intake/sportsdataverse_mlb.py
```

### Add run projection

After:

```text
MLB Game ID Predictions
```

add:

```text
MLB Build Run Projection
```

running:

```text
python docs/win/baseball/mlb/scripts/00_intake/build_run_projection.py
```

### Keep

Keep:

```text
MLB Merge Intake
MLB Build Juice Files
MLB Compute Edges
MLB Compute EV Kelly
MLB Select Bets
MLB Select Morning Bets
```

`MLB Build Juice Files` can be renamed later, but its first migration can keep the existing workflow label.

### Remove from execution

Remove these three workflow steps:

```text
MLB Moneyline Juice
MLB Run Line Juice
MLB Total Juice
```

### Resulting calculation order

The production order should be:

```text
MLB Odds Pull
MLB Odds Parse
MLB Name Normalization Pre
MLB DRAT Scraper
MLB Transform Baseball
MLB Name Normalization Post
MLB Scrape Raw
MLB Build Games List
MLB SportsDataverse Features
MLB Fetch Park Weather
MLB Build Game Weather
MLB Enrich Game Context
MLB Game ID Predictions
MLB Build Run Projection
MLB Merge Intake
MLB Build Juice Files
MLB Compute Edges
MLB Compute EV Kelly
MLB Select Bets
MLB Select Morning Bets
```

The sportsbook odds are available throughout the merge/market stages, but they do not enter the run-projection model.

---

# TODO 15 — Add a separate model-training workflow; do not retrain every morning

### New workflow

Create:

```text
.github/workflows/mlb_05_model_train.yml
```

### Trigger

Use:

```text
workflow_dispatch
```

Do not attach initial training to the daily morning workflow.

### Steps

The model-training workflow should execute:

```text
checkout
setup Python 3.11
install requirements
install vendored SportsDataverse
build/backfill leakage-safe SDV feature files
build_run_training_set.py
train_run_model.py
run model validation tests
```

The workflow should produce:

```text
models/run_projection/*.joblib
models/run_projection/*_metadata.json
modeling/data/mlb_run_training_set.csv
modeling/reports/*
```

Decide separately which generated training/report files belong in Git. The production model artifacts and metadata must be available to the morning workflow before `build_run_projection.py` can run.

---

# TODO 16 — Add deterministic validation tests before changing selection behavior

### New test file

Create:

```text
tests/mlb/test_probability_ev_kelly.py
```

### Required tests

#### Moneyline

For every test row:

```text
home_model_prob_moneyline + away_model_prob_moneyline == 1
```

Check:

```text
EV = p * decimal - 1
Kelly sign == EV sign before clipping
```

#### Run line

For a complementary `-1.5/+1.5` market:

```text
home_model_prob_run_line + away_model_prob_run_line == 1
```

Check that swapping home/away expected runs swaps the corresponding cover probabilities.

#### Half-run total

Check:

```text
p_push = 0
p_over + p_under = 1
```

#### Whole-run total

Check:

```text
p_over + p_under + p_push = 1
```

and verify the push-aware EV formula.

#### Price independence

For a fixed:

```text
model_home_runs
model_away_runs
market line
```

calculate model probabilities using two different sportsbook price sets.

The model probabilities must be identical.

Only:

```text
break-even probability
edge
EV
Kelly
```

may change with the offered price.

#### EV/Kelly consistency

Across generated test probabilities and prices:

```text
EV > 0  iff raw Kelly > 0
EV < 0  iff raw Kelly < 0
```

No positive-EV/zero-Kelly state should exist except a tiny floating-point value at zero tolerance.

---

# TODO 17 — Produce a model comparison report before treating the new probabilities as authoritative

### New directory

```text
docs/win/baseball/mlb/modeling/reports/
```

### New script

Create:

```text
docs/win/baseball/mlb/scripts/modeling/evaluate_run_model.py
```

### Compare on the untouched chronological test period

Compare:

```text
DRatings home projected runs
vs
new model_home_runs

DRatings away projected runs
vs
new model_away_runs
```

Then derive both systems' market probabilities and compare:

```text
moneyline probability calibration
run-line probability calibration
total probability calibration
```

### Required report outputs

Write:

```text
run_prediction_metrics.csv
moneyline_calibration.csv
run_line_calibration.csv
total_calibration.csv
probability_log_loss.csv
model_comparison_summary.md
```

### Required questions the report must answer

For the untouched test set:

```text
Does the new model improve home-run prediction error?
Does the new model improve away-run prediction error?
Are predicted moneyline probabilities calibrated?
Are predicted run-line probabilities calibrated?
Are predicted total probabilities calibrated?
Does increasing predicted probability correspond to increasing observed win rate?
Does higher predicted EV correspond to higher realized return, or is EV overstated?
Does Kelly increase monotonically with the actual model edge?
How often does the rebuilt system prefer -1.5 versus +1.5 when both are evaluated?
```

Do not tune the model using the final test data after this report is produced.

---

# TODO 18 — Final production invariants

Before the rebuilt pipeline is considered complete, all of these must be true.

### Model

- [ ] No sportsbook odds are included in the run-model feature matrix.
- [ ] Historical SDV features use only dates before the game.
- [ ] Original DRatings projections remain preserved for comparison.
- [ ] `model_home_runs` and `model_away_runs` are finite and non-negative.
- [ ] Model metadata records its feature list and training cutoff.

### Market probabilities

- [ ] Moneyline model probabilities sum to `1`.
- [ ] Run-line model probabilities sum to `1`.
- [ ] Whole-number total win/loss/push probabilities sum to `1`.
- [ ] Half-number total push probability is `0`.
- [ ] Changing DK odds does not change any model probability.

### EV / Kelly

- [ ] EV is calculated directly from model probability and offered price.
- [ ] Kelly is calculated from the same model probability and price.
- [ ] Context no longer directly modifies EV.
- [ ] Positive EV cannot coexist with zero/negative raw Kelly.
- [ ] Whole-number total EV does not count a push as a loss.

### Run-line selection

- [ ] Both `+1.5` and `-1.5` candidates are calculated for every supported run-line game.
- [ ] Both sides use the same mathematical process.
- [ ] Final side choice is based on value, not highest probability.
- [ ] The selection audit records both sides before the final choice.

### Pipeline

- [ ] SportsDataverse runs after the authoritative games list exists.
- [ ] `build_run_projection.py` runs after game IDs and SDV features exist.
- [ ] `merge_intake.py` consumes model-projection files.
- [ ] `build_juice_files.py` writes directly to `02_juice`.
- [ ] The three old sportsbook-price probability-adjustment steps no longer execute.
- [ ] `compute_edges.py` calculates probability edge only.
- [ ] `compute_ev_kelly.py` is the only EV/Kelly calculation stage.

---

# Recommended implementation sequence

Implement and validate in this order because each step establishes an interface required by the next one:

1. **TODO 1:** canonical probability/output column contract.
2. **TODO 2:** remove price-based probability adjustment from execution.
3. **TODO 3:** rebuild market-probability generation using the current DRatings runs first.
4. **TODO 4:** redefine edge as probability edge.
5. **TODO 5:** repair EV/Kelly and push handling.
6. **TODO 16:** add deterministic math tests and confirm the repaired current pipeline passes them.
7. **TODO 7:** add historical as-of SDV backfill capability.
8. **TODO 8:** enforce leakage-safe feature rules.
9. **TODO 9:** build the model training dataset.
10. **TODO 10:** train and save run models.
11. **TODO 11:** create production run projection.
12. **TODO 12:** point merge at model projection.
13. **TODO 13:** make run-line candidate comparison value-based and side-neutral.
14. **TODO 14:** update the morning workflow.
15. **TODO 15:** add the separate training workflow.
16. **TODO 17:** produce the out-of-sample model comparison report.
17. **TODO 18:** verify every production invariant.

This sequence intentionally separates **repairing the existing EV/Kelly mathematics** from **introducing a new trained SportsDataverse run model**. That prevents a model change from hiding a probability/EV implementation error.

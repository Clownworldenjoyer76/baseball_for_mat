# docs/win/baseball/mlb/scripts/00_parsing/transform_raw_park_factors.py
#!/usr/bin/env python3

import argparse
import csv
import re
import sys
from pathlib import Path


FINAL_COLUMNS = [
    "Team",
    "Venue",
    "Year",
    "Park Factor",
    "wOBAcon",
    "xwOBAcon",
    "BACON",
    "xBACON",
    "HardHit",
    "R",
    "OBP",
    "H",
    "1B",
    "2B",
    "3B",
    "HR",
    "BB",
    "SO",
    "PA",
    "venue_id",
    "team_id",
]

RAW_REQUIRED_COLUMNS = [
    "Team",
    "Venue",
    "Year",
    "Park Factor",
    "wOBAcon",
    "xwOBAcon",
    "BACON",
    "xBACON",
    "HardHit",
    "R",
    "OBP",
    "H",
    "1B",
    "2B",
    "3B",
    "HR",
    "BB",
    "SO",
    "PA",
]

NUMERIC_FACTOR_COLUMNS = [
    "Park Factor",
    "wOBAcon",
    "xwOBAcon",
    "BACON",
    "xBACON",
    "HardHit",
    "R",
    "OBP",
    "H",
    "1B",
    "2B",
    "3B",
    "HR",
    "BB",
    "SO",
]

TEAM_NAME_ALIASES = {
    "A's": "Athletics",
    "A’s": "Athletics",
    "Athletics": "Athletics",
    "D-backs": "D-backs",
    "Diamondbacks": "D-backs",
    "Bluejays": "Blue Jays",
    "BlueJays": "Blue Jays",
    "WhiteSox": "White Sox",
    "RedSox": "Red Sox",
}

VENUE_ALIASES = {
    "Dodger Stadium": "UNIQLO Field at Dodger Stadium",
    "UNIQLO Field at Dodger Stadium": "UNIQLO Field at Dodger Stadium",
    "loanDepot Park": "loanDepot park",
    "loanDepot park": "loanDepot park",
    "Rate Field": "Rate Field",
    "Guaranteed Rate Field": "Rate Field",
}


def clean(value):
    if value is None:
        return ""
    return str(value).strip().strip("\ufeff")


def normalize_key(value):
    return (
        clean(value)
        .lower()
        .replace(".", "")
        .replace("-", " ")
        .replace("_", " ")
        .replace("’", "'")
        .strip()
    )


def normalize_team(team):
    team = clean(team)
    return TEAM_NAME_ALIASES.get(team, team)


def normalize_venue(venue):
    venue = clean(venue)
    return VENUE_ALIASES.get(venue, venue)


def split_line(line):
    line = clean(line)

    if "\t" in line:
        return [clean(part) for part in line.split("\t")]

    if "," in line and '"' in line:
        return [clean(part) for part in next(csv.reader([line]), [])]

    return [clean(part) for part in re.split(r"\s{2,}", line) if clean(part)]


def looks_like_header(parts):
    normalized = [clean(part) for part in parts]
    return (
        "Team" in normalized
        and "Venue" in normalized
        and "Year" in normalized
        and "Park Factor" in normalized
        and "PA" in normalized
    )


def _find_park_table_header(lines):
    for index, line in enumerate(lines):
        parts = split_line(line)
        if looks_like_header(parts):
            if parts and parts[0] in {"Rk.", "Rk", "Rank"}:
                parts = parts[1:]
            return index, parts
    raise ValueError("Could not find park-factor table header row in raw input.")


def _park_row_from_line(line, raw_line_number, header_parts):
    if not clean(line):
        return None
    parts = split_line(line)
    if not parts:
        return None
    if parts[0].isdigit() and len(parts) == len(header_parts) + 1:
        parts = parts[1:]
    if len(parts) < len(header_parts):
        return None
    if len(parts) > len(header_parts):
        raise ValueError(
            f"row {raw_line_number}: too many columns. "
            f"Expected {len(header_parts)}, got {len(parts)}. Line: {line}"
        )
    row = {
        header_parts[index]: clean(parts[index])
        for index in range(len(header_parts))
    }
    if not clean(row.get("Team")) or not clean(row.get("Venue")):
        return None
    return row


def extract_table_rows_from_raw_dump(raw_path):
    text = raw_path.read_text(encoding="utf-8-sig", errors="replace")
    lines = [line.rstrip("\r\n") for line in text.splitlines()]
    header_index, header_parts = _find_park_table_header(lines)
    missing = [col for col in RAW_REQUIRED_COLUMNS if col not in header_parts]
    if missing:
        raise ValueError(
            "Raw park-factor table header is missing required columns: "
            + ", ".join(missing)
        )
    rows = []
    for raw_line_number, line in enumerate(
        lines[header_index + 1:],
        start=header_index + 2,
    ):
        row = _park_row_from_line(line, raw_line_number, header_parts)
        if row is not None:
            rows.append(row)
    if not rows:
        raise ValueError("No data rows found after park-factor table header.")
    return rows

def read_csv_dicts(path, delimiter=","):
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        rows = []

        if not reader.fieldnames:
            raise ValueError(f"No header found in file: {path}")

        reader.fieldnames = [clean(x) for x in reader.fieldnames]

        for row in reader:
            cleaned = {}

            for key, value in row.items():
                cleaned[clean(key)] = clean(value)

            if any(cleaned.values()):
                rows.append(cleaned)

    return rows


def require_columns(rows, required_columns, file_label):
    if not rows:
        raise ValueError(f"{file_label} has no data rows.")

    actual = set(rows[0].keys())
    missing = [col for col in required_columns if col not in actual]

    if missing:
        raise ValueError(
            f"{file_label} is missing required columns: {', '.join(missing)}"
        )


def add_team_key(by_team, key_source, row, team_id, venue_id, venue_name):
    name = normalize_team(clean(key_source))

    if not name:
        return

    key = normalize_key(name)

    if key in by_team:
        existing = by_team[key]

        if existing["team_id"] != team_id:
            raise ValueError(
                f"Duplicate team map key with conflicting team_id: {name}"
            )

    by_team[key] = {
        "team": name,
        "team_id": team_id,
        "venue_id": venue_id,
        "venue_name": venue_name,
        "canonical_team_name": clean(row.get("team_name")),
    }


def load_team_map(team_map_path):
    rows = read_csv_dicts(team_map_path, delimiter=",")

    require_columns(
        rows,
        [
            "team_id",
            "team_name",
            "name",
            "club_name",
            "short_name",
            "venue_id",
            "venue_name",
        ],
        "Team map",
    )

    by_team = {}

    for row in rows:
        team_id = clean(row.get("team_id"))
        venue_id = clean(row.get("venue_id"))
        venue_name = clean(row.get("venue_name"))

        if not team_id:
            continue

        possible_names = [
            row.get("team_name"),
            row.get("name"),
            row.get("club_name"),
            row.get("short_name"),
        ]

        for name in possible_names:
            add_team_key(
                by_team,
                name,
                row,
                team_id,
                venue_id,
                venue_name,
            )

    return by_team


def load_canonical_teams(team_map_path):
    rows = read_csv_dicts(team_map_path, delimiter=",")
    require_columns(rows, ["team_id", "team_name"], "Team map")

    teams = {}

    for row in rows:
        team_id = clean(row.get("team_id"))
        team_name = normalize_team(clean(row.get("team_name")))

        if team_id and team_name:
            teams[team_id] = team_name

    return teams


def load_venue_map(venue_map_path):
    rows = read_csv_dicts(venue_map_path, delimiter=",")
    require_columns(rows, ["venue_id", "venue_name"], "Venue map")

    by_venue = {}

    for row in rows:
        venue_id = clean(row.get("venue_id"))
        venue_name = normalize_venue(clean(row.get("venue_name")))

        if not venue_id or not venue_name:
            continue

        key = normalize_key(venue_name)

        if key in by_venue:
            existing = by_venue[key]

            if existing["venue_id"] != venue_id:
                continue

        by_venue[key] = {
            "venue_id": venue_id,
            "venue_name": venue_name,
        }

    return by_venue


def validate_raw_row(row, row_number):
    errors = []

    for col in RAW_REQUIRED_COLUMNS:
        if clean(row.get(col)) == "":
            errors.append(f"row {row_number}: missing value in {col}")

    for col in NUMERIC_FACTOR_COLUMNS:
        value = clean(row.get(col))

        if value == "":
            continue

        if not value.lstrip("-").isdigit():
            errors.append(
                f"row {row_number}: non-integer value in {col}: {value}"
            )

    pa = clean(row.get("PA"))

    if pa:
        pa_as_int = pa.replace(",", "")

        if not pa_as_int.isdigit():
            errors.append(f"row {row_number}: invalid PA value: {pa}")

    return errors


def _transform_park_factor_row(raw, index, team_map, venue_map, seen_keys):
    errors = list(validate_raw_row(raw, index))
    team = normalize_team(raw.get("Team"))
    venue = normalize_venue(raw.get("Venue"))
    year = clean(raw.get("Year"))
    row_key = (normalize_key(team), normalize_key(venue), year)
    if row_key in seen_keys:
        errors.append(
            f"row {index}: duplicate Team/Venue/Year: {team} | {venue} | {year}"
        )
        return None, errors
    seen_keys.add(row_key)
    team_match = team_map.get(normalize_key(team))
    venue_match = venue_map.get(normalize_key(venue))
    if not team_match:
        errors.append(f"row {index}: no team_id match for Team: {team}")
        return None, errors
    if not venue_match:
        errors.append(f"row {index}: no venue_id match for Venue: {venue}")
        return None, errors
    team_venue = clean(team_match.get("venue_id"))
    venue_venue = clean(venue_match.get("venue_id"))
    if team_venue != venue_venue:
        errors.append(
            f"row {index}: team map venue_id does not match venue map venue_id "
            f"for {team} | {venue}. team map={team_venue}, venue map={venue_venue}"
        )
        return None, errors
    out = {col: clean(raw.get(col)) for col in RAW_REQUIRED_COLUMNS}
    out.update({
        "Team": team,
        "Venue": venue,
        "venue_id": venue_venue,
        "team_id": clean(team_match.get("team_id")),
    })
    return out, errors


def _park_transform_warnings(output_rows, canonical_teams, matched_team_ids):
    warnings = []
    missing_teams = sorted(
        team_name for team_id, team_name in canonical_teams.items()
        if team_id not in matched_team_ids
    )
    if len(output_rows) != 30:
        warnings.append(
            f"output row count is {len(output_rows)}, expected 30 for full MLB park-factor file"
        )
    if missing_teams:
        warnings.append(
            "teams in team map but missing from raw input: " + ", ".join(missing_teams)
        )
    return warnings


def _write_park_transform_csv(output_path, output_rows):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=FINAL_COLUMNS,
            quoting=csv.QUOTE_ALL,
            lineterminator="\n",
        )
        writer.writeheader()
        for row in output_rows:
            writer.writerow({col: row.get(col, "") for col in FINAL_COLUMNS})


def _write_park_transform_audit(
    audit_path, raw_path, team_map_path, venue_map_path, output_path,
    raw_rows, output_rows, errors, warnings,
):
    lines = [
        "PARK FACTOR TRANSFORM AUDIT", "",
        f"raw input: {raw_path}",
        f"team map: {team_map_path}",
        f"venue map: {venue_map_path}",
        f"output: {output_path}", "",
        f"raw rows extracted: {len(raw_rows)}",
        f"output rows: {len(output_rows)}",
        f"errors: {len(errors)}",
        f"warnings: {len(warnings)}", "",
    ]
    if errors:
        lines.extend(["ERRORS", *[f"- {error}" for error in errors], ""])
    if warnings:
        lines.extend(["WARNINGS", *[f"- {warning}" for warning in warnings], ""])
    lines.append("OUTPUT ROWS")
    lines.extend(
        f"- {row['Team']} | {row['Venue']} | {row['Year']} | "
        f"venue_id={row['venue_id']} | team_id={row['team_id']}"
        for row in output_rows
    )
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def transform(raw_path, team_map_path, venue_map_path, output_path, audit_path):
    raw_rows = extract_table_rows_from_raw_dump(raw_path)
    require_columns(raw_rows, RAW_REQUIRED_COLUMNS, "Raw park-factor file")
    team_map = load_team_map(team_map_path)
    canonical_teams = load_canonical_teams(team_map_path)
    venue_map = load_venue_map(venue_map_path)
    output_rows = []
    errors = []
    seen_keys = set()
    matched_team_ids = set()
    for index, raw in enumerate(raw_rows, start=2):
        out, row_errors = _transform_park_factor_row(
            raw, index, team_map, venue_map, seen_keys
        )
        errors.extend(row_errors)
        if out is not None:
            matched_team_ids.add(out["team_id"])
            output_rows.append(out)
    warnings = _park_transform_warnings(
        output_rows, canonical_teams, matched_team_ids
    )
    _write_park_transform_csv(output_path, output_rows)
    _write_park_transform_audit(
        audit_path, raw_path, team_map_path, venue_map_path, output_path,
        raw_rows, output_rows, errors, warnings,
    )
    if errors:
        print(
            f"FAILED: {len(errors)} error(s). Audit written to: {audit_path}",
            file=sys.stderr,
        )
        return 1
    print(f"OK: wrote {len(output_rows)} rows to {output_path}")
    print(f"Audit written to: {audit_path}")
    if warnings:
        print(
            f"WARNING: {len(warnings)} warning(s). Review audit before using output."
        )
    return 0

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Transform raw MLB park-factor export into final quoted CSV "
            "with venue_id and team_id."
        )
    )

    parser.add_argument(
        "--raw",
        required=True,
        help="Path to raw pasted park-factor file.",
    )

    parser.add_argument(
        "--team-map",
        default="docs/win/baseball/mlb/maps/mlb_team_ids.csv",
        help=(
            "Path to "
            "docs/win/baseball/mlb/maps/mlb_team_ids.csv."
        ),
    )

    parser.add_argument(
        "--venue-map",
        default="docs/win/baseball/mlb/maps/mlb_venue_ids.csv",
        help=(
            "Path to "
            "docs/win/baseball/mlb/maps/mlb_venue_ids.csv."
        ),
    )

    parser.add_argument(
        "--out",
        required=True,
        help="Output CSV path.",
    )

    parser.add_argument(
        "--audit",
        default=None,
        help=(
            "Audit text output path. Defaults to output path "
            "with .audit.txt suffix."
        ),
    )

    args = parser.parse_args()

    raw_path = Path(args.raw)
    team_map_path = Path(args.team_map)
    venue_map_path = Path(args.venue_map)
    output_path = Path(args.out)

    if args.audit:
        audit_path = Path(args.audit)
    else:
        audit_path = output_path.with_suffix(
            output_path.suffix + ".audit.txt"
        )

    for label, path in [
        ("raw", raw_path),
        ("team map", team_map_path),
        ("venue map", venue_map_path),
    ]:
        if not path.exists():
            print(
                f"FAILED: {label} file does not exist: {path}",
                file=sys.stderr,
            )
            return 1

    return transform(
        raw_path=raw_path,
        team_map_path=team_map_path,
        venue_map_path=venue_map_path,
        output_path=output_path,
        audit_path=audit_path,
    )


if __name__ == "__main__":
    raise SystemExit(main())

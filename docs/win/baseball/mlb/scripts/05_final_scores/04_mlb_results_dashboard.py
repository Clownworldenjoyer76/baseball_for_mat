#!/usr/bin/env python3
# docs/win/baseball/mlb/scripts/05_final_scores/04_mlb_results_dashboard.py
#
# Builds a single self-contained MLB dashboard with two source views:
#   - MLB                -> morning graded/report tree
#   - MLB · WITH LINEUPS -> pregame/non-morning graded/report tree
#
# Inputs:
#   docs/win/baseball/mlb/05_final_scores/morning/mlb_summary_overall.csv
#   docs/win/baseball/mlb/05_final_scores/morning/reports/{overview,moneyline,run_line,totals}/*.csv
#   docs/win/baseball/mlb/05_final_scores/mlb_summary_overall.csv
#   docs/win/baseball/mlb/05_final_scores/reports/{overview,moneyline,run_line,totals}/*.csv
#
# Output:
#   frontend/baseball_dashboard.html
#
# Log:
#   docs/win/baseball/mlb/errors/05_final_scores/04_mlb_results_dashboard.txt

from __future__ import annotations

from datetime import datetime, UTC
from pathlib import Path
import html
import json
import traceback

import pandas as pd

BASE = Path("docs/win/baseball/mlb/05_final_scores")
OUTPUT_FILE = Path("frontend/baseball_dashboard.html")
LEAGUE_OUTPUT_FILE = Path("frontend/mlb_dashboard.html")
ERROR_DIR = Path("docs/win/baseball/mlb/errors/05_final_scores")
LOG_FILE = ERROR_DIR / "04_mlb_results_dashboard.txt"

FEEDS = {
    "mlb": {
        "label": "MLB",
        "root": BASE / "morning",
    },
    "mlb_lineups": {
        "label": "MLB WITH LINEUPS",
        "root": BASE,
    },
}

MARKETS = {
    "moneyline": {
        "label": "Moneyline",
        "directory": "moneyline",
        "file_key": "moneyline",
        "dimensions": ["ev", "odds", "kelly", "win_prob"],
    },
    "run_line": {
        "label": "Run Line",
        "directory": "run_line",
        "file_key": "run_line",
        "dimensions": ["ev", "odds", "kelly", "win_prob", "side"],
    },
    "total": {
        "label": "Total",
        "directory": "totals",
        "file_key": "total",
        "dimensions": ["ev", "odds", "kelly", "win_prob", "total_range", "side"],
    },
}

ERROR_DIR.mkdir(parents=True, exist_ok=True)

RUN_STARTED = datetime.now(UTC)
WARNING_COUNT = 0
ERROR_COUNT = 0
INPUT_FILE_COUNT = 0
INPUT_ROW_COUNT = 0
OUTPUT_FILE_COUNT = 0
OUTPUT_ROW_COUNT = 0
INPUT_FILES_SEEN: set[str] = set()

with LOG_FILE.open("w", encoding="utf-8") as f:
    f.write("=== 04_mlb_results_dashboard ===\n")
    f.write(f"START_TIMESTAMP_UTC: {RUN_STARTED.isoformat()}\n")


def _now() -> str:
    return datetime.now(UTC).isoformat()


def log(level: str, message: str) -> None:
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(f"{_now()} | {level} | {message}\n")


def warn(message: str) -> None:
    global WARNING_COUNT
    WARNING_COUNT += 1
    log("WARNING", message)


def error(message: str) -> None:
    global ERROR_COUNT
    ERROR_COUNT += 1
    log("ERROR", message)


def log_input(path: Path, rows: int, exists: bool) -> None:
    global INPUT_FILE_COUNT, INPUT_ROW_COUNT
    key = str(path)
    if key in INPUT_FILES_SEEN:
        return
    INPUT_FILES_SEEN.add(key)
    INPUT_FILE_COUNT += 1
    INPUT_ROW_COUNT += rows
    log("INFO", f"INPUT | file={path} | exists={int(exists)} | rows={rows}")


def log_output(path: Path, rows: int, bytes_written: int) -> None:
    global OUTPUT_FILE_COUNT, OUTPUT_ROW_COUNT
    OUTPUT_FILE_COUNT += 1
    OUTPUT_ROW_COUNT += rows
    log("INFO", f"OUTPUT | file={path} | rows={rows} | bytes={bytes_written}")


def finish(status: str) -> None:
    ended = datetime.now(UTC)
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(f"INPUT_SUMMARY | files={INPUT_FILE_COUNT} | rows={INPUT_ROW_COUNT}\n")
        f.write(f"OUTPUT_SUMMARY | files={OUTPUT_FILE_COUNT} | rows={OUTPUT_ROW_COUNT}\n")
        f.write(f"WARNING_COUNT: {WARNING_COUNT}\n")
        f.write(f"ERROR_COUNT: {ERROR_COUNT}\n")
        f.write(f"END_TIMESTAMP_UTC: {ended.isoformat()}\n")
        f.write(f"STATUS: {status}\n")


def safe_read(path: Path, *, required: bool = False) -> pd.DataFrame:
    if not path.exists():
        log_input(path, 0, False)
        if required:
            warn(f"Required dashboard input missing: {path}")
        else:
            log("INFO", f"Optional dashboard input missing: {path}")
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
        log_input(path, len(df), True)
        return df
    except Exception as exc:
        log_input(path, 0, True)
        warn(f"Unable to read dashboard input {path}: {type(exc).__name__}: {exc}")
        return pd.DataFrame()


def _clean_value(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def df_to_records(df: pd.DataFrame) -> list[dict]:
    if df.empty:
        return []
    return [
        {key: _clean_value(value) for key, value in row.items()}
        for row in df.to_dict(orient="records")
    ]


def first_row_dict(df: pd.DataFrame) -> dict:
    rows = df_to_records(df.head(1))
    return rows[0] if rows else {}


def collect_feed_data(feed_key: str) -> dict:
    cfg = FEEDS[feed_key]
    root: Path = cfg["root"]
    reports = root / "reports"
    overview = reports / "overview"

    data = {
        "feed": feed_key,
        "label": cfg["label"],
        "headline": first_row_dict(
            safe_read(overview / "mlb_summary_overall.csv", required=True)
        ),
        "by_market_summary": df_to_records(
            safe_read(root / "mlb_summary_overall.csv", required=True)
        ),
        "overview": {
            "by_market": df_to_records(safe_read(overview / "mlb_summary_by_market.csv")),
            "by_side_group": df_to_records(safe_read(overview / "mlb_summary_by_side_group.csv")),
            "by_date": df_to_records(safe_read(overview / "mlb_summary_by_date.csv")),
            "by_day_night": df_to_records(safe_read(overview / "mlb_summary_by_day_night.csv")),
            "by_low_confidence": df_to_records(
                safe_read(overview / "mlb_summary_by_low_confidence.csv")
            ),
        },
        "markets": {},
    }

    for market_key, market_cfg in MARKETS.items():
        market_dir = reports / market_cfg["directory"]
        file_key = market_cfg["file_key"]
        market_data = {"by": {}, "by_side": {}}

        for dimension in market_cfg["dimensions"]:
            base_name = f"mlb_{file_key}_by_{dimension}"
            market_data["by"][dimension] = df_to_records(
                safe_read(market_dir / f"{base_name}.csv")
            )
            market_data["by_side"][dimension] = df_to_records(
                safe_read(market_dir / f"{base_name}_home_away_summary.csv")
            )

        data["markets"][market_key] = market_data

    return data


CSS = r"""
:root {
  --bg:#0e1117;
  --panel:#161b22;
  --panel2:#1c232c;
  --text:#e6edf3;
  --muted:#8b949e;
  --accent:#58a6ff;
  --good:#3fb950;
  --bad:#f85149;
  --border:#30363d;
  --table-border:#46515e;
}
*{box-sizing:border-box}
html,body{margin:0;padding:0;background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif}
header{padding:18px 24px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:baseline;gap:12px;flex-wrap:wrap}
header h1{font-size:20px;margin:0}
header .ts{font-size:12px;color:var(--muted)}
.feed-bar{position:sticky;top:0;z-index:10;padding:10px 24px;background:var(--panel);border-bottom:1px solid var(--border);display:flex;gap:6px;align-items:center;flex-wrap:wrap}
.feed-bar .lbl{font-size:12px;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);margin-right:6px}
.feed-btn{background:var(--panel2);border:1px solid var(--border);color:var(--text);padding:6px 14px;border-radius:4px;cursor:pointer;font-size:13px}
.feed-btn.active{background:var(--accent);border-color:var(--accent);color:#0e1117;font-weight:600}
main{padding:18px 24px;max-width:1500px;margin:0 auto}
.feed-section{display:none}.feed-section.active{display:block}
h2{font-size:16px;margin:24px 0 8px;border-bottom:1px solid var(--border);padding-bottom:4px}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px;margin:12px 0 4px}
.kpi{background:var(--panel);border:1px solid var(--border);border-radius:6px;padding:12px}
.kpi .label{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.05em}
.kpi .value{font-size:22px;margin-top:4px;font-weight:600}.kpi .value.good{color:var(--good)}.kpi .value.bad{color:var(--bad)}
.tabs{display:flex;gap:4px;margin:16px 0 0;flex-wrap:wrap}.tab{background:var(--panel);border:1px solid var(--border);padding:6px 12px;border-radius:6px 6px 0 0;cursor:pointer;color:var(--muted);font-size:13px}.tab.active{background:var(--panel2);color:var(--text);border-bottom-color:var(--panel2)}
.tab-body{background:var(--panel2);border:1px solid var(--border);border-top:none;padding:14px;border-radius:0 6px 6px 6px}
.controls{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:10px}.controls label{font-size:12px;color:var(--muted)}.controls select{background:var(--panel);color:var(--text);border:1px solid var(--border);padding:4px 8px;border-radius:4px;font-size:13px}
.scroll{max-height:60vh;overflow:auto;border:1px solid var(--table-border);border-radius:6px}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{padding:7px 9px;text-align:center;border:1px solid var(--table-border);white-space:nowrap;vertical-align:middle}
th{position:sticky;top:0;background:var(--panel2);color:var(--muted);text-transform:uppercase;font-size:11px;letter-spacing:.05em;cursor:pointer;user-select:none}
th .arrow{opacity:.4;margin-left:4px}
th.sorted .arrow{opacity:1;color:var(--accent)}
td.num{text-align:center;font-variant-numeric:tabular-nums}
.pos{color:var(--good)}
.neg{color:var(--bad)}
.muted{color:var(--muted)}
td.win-pct-strong{background:rgba(63,185,80,.24);color:#7ee787;font-weight:700}
td.win-pct-green{background:rgba(63,185,80,.16);color:#56d364;font-weight:600}
td.win-pct-light{background:rgba(63,185,80,.09);color:#8ddb8c;font-weight:600}
td.win-pct-neutral{background:rgba(139,148,158,.09);color:#c9d1d9}
td.win-pct-red{background:rgba(248,81,73,.14);color:#ff7b72;font-weight:600}
"""

JS = r"""
function fmtPct(v){if(v==null||isNaN(v))return '';return (Number(v)*100).toFixed(2)+'%';}
function fmtNum(v,d){if(v==null||isNaN(v))return '';return Number(v).toFixed(d);}
function fmtInt(v){if(v==null||isNaN(v))return '';return Number(v).toLocaleString();}
function signedClass(v){if(v==null||isNaN(v))return '';return Number(v)>0?'pos':(Number(v)<0?'neg':'');}
function winPctClass(v){
  if(v==null||isNaN(v))return '';
  const pct=Number(v);
  if(pct>=0.80)return 'win-pct-strong';
  if(pct>=0.70)return 'win-pct-green';
  if(pct>=0.60)return 'win-pct-light';
  if(pct>=0.50)return 'win-pct-neutral';
  return 'win-pct-red';
}
function showTab(host,key){host.querySelectorAll(':scope > .tabs .tab').forEach(el=>el.classList.toggle('active',el.dataset.key===key));host.querySelectorAll(':scope > .tab-body > .tab-panel').forEach(el=>el.style.display=(el.dataset.key===key?'':'none'));}

function renderTable(data,columns,container){
  if(!data||!data.length){container.innerHTML='<div class="muted">No rows.</div>';return;}
  const wrap=document.createElement('div');wrap.className='scroll';
  const table=document.createElement('table');const thead=document.createElement('thead');const header=document.createElement('tr');const tbody=document.createElement('tbody');
  let sortCol=null,sortDir='desc';
  columns.forEach(c=>{const th=document.createElement('th');th.innerHTML=c.label+' <span class="arrow">&#9662;</span>';th.onclick=()=>{if(sortCol===c.key)sortDir=(sortDir==='asc'?'desc':'asc');else{sortCol=c.key;sortDir='desc';}draw();thead.querySelectorAll('th').forEach(h=>h.classList.remove('sorted'));th.classList.add('sorted');};header.appendChild(th);});
  thead.appendChild(header);
  function draw(){
    const rows=data.slice();
    if(sortCol){rows.sort((a,b)=>{const av=a[sortCol],bv=b[sortCol];if(av==null)return 1;if(bv==null)return -1;if(!isNaN(Number(av))&&!isNaN(Number(bv)))return sortDir==='asc'?Number(av)-Number(bv):Number(bv)-Number(av);return sortDir==='asc'?String(av).localeCompare(String(bv)):String(bv).localeCompare(String(av));});}
    tbody.innerHTML='';
    rows.forEach(row=>{const tr=document.createElement('tr');columns.forEach(c=>{const td=document.createElement('td');const v=row[c.key];let cls='';if(c.fmt==='int'){td.classList.add('num');td.textContent=fmtInt(v);}else if(c.fmt==='pct'){td.classList.add('num');td.textContent=fmtPct(v);if(c.key==='Win_Pct'){const winCls=winPctClass(v);if(winCls)td.classList.add(winCls);}cls=c.color?signedClass(v):'';}else if(c.fmt==='num'){td.classList.add('num');td.textContent=fmtNum(v,c.decimals==null?2:c.decimals);cls=c.color?signedClass(v):'';}else{td.textContent=(v==null?'':String(v));}if(cls)td.classList.add(cls);tr.appendChild(td);});tbody.appendChild(tr);});
  }
  draw();table.appendChild(thead);table.appendChild(tbody);wrap.appendChild(table);container.innerHTML='';container.appendChild(wrap);
}

const METRIC_COLUMNS=[
  {key:'variable',label:'Bucket'},
  {key:'Win',label:'W',fmt:'int'},
  {key:'Loss',label:'L',fmt:'int'},
  {key:'Push',label:'P',fmt:'int'},
  {key:'Total',label:'Total',fmt:'int'},
  {key:'Win_Pct',label:'Win %',fmt:'pct'},
  {key:'units',label:'Units',fmt:'num',decimals:2,color:true},
  {key:'ROI_Excluding_Pushes',label:'ROI excl. pushes',fmt:'pct',color:true},
  {key:'ROI_Including_Pushes',label:'ROI incl. pushes',fmt:'pct',color:true},
  {key:'avg_ev',label:'Avg EV',fmt:'pct'},
  {key:'avg_odds',label:'Avg odds',fmt:'num',decimals:0}
];
const METRIC_SIDE_COLUMNS=[{key:'side_group',label:'Side'},...METRIC_COLUMNS];
const OVERVIEW_COLUMNS=[
  {key:'Win',label:'W',fmt:'int'},{key:'Loss',label:'L',fmt:'int'},{key:'Push',label:'P',fmt:'int'},{key:'Total',label:'Total',fmt:'int'},
  {key:'Win_Pct',label:'Win %',fmt:'pct'},{key:'units',label:'Units',fmt:'num',decimals:2,color:true},{key:'ROI_Excluding_Pushes',label:'ROI excl. pushes',fmt:'pct',color:true},{key:'ROI_Including_Pushes',label:'ROI incl. pushes',fmt:'pct',color:true},{key:'avg_ev',label:'Avg EV',fmt:'pct'},{key:'avg_odds',label:'Avg odds',fmt:'num',decimals:0}
];

function selectFeed(feed){document.querySelectorAll('.feed-btn').forEach(b=>b.classList.toggle('active',b.dataset.feed===feed));document.querySelectorAll('.feed-section').forEach(s=>s.classList.toggle('active',s.dataset.feed===feed));try{localStorage.setItem('baseball_dash_feed',feed);}catch(e){}}

function buildFeedSection(feed,data){
  const section=document.querySelector('.feed-section[data-feed="'+feed+'"]');if(!section)return;
  const h=data.headline||{};
  const kpi=(label,value,fmt)=>{let disp='N/A',cls='';if(value!==null&&value!==undefined&&value!==''){if(fmt==='pct'){disp=fmtPct(value);cls=signedClass(value);}else if(fmt==='int'){disp=fmtInt(value);}else if(fmt==='signed'){disp=(Number(value)>=0?'+':'')+fmtNum(value,2);cls=signedClass(value);}else if(fmt==='num'){disp=fmtNum(value,2);}else{disp=String(value);}}return '<div class="kpi"><div class="label">'+label+'</div><div class="value '+cls+'">'+disp+'</div></div>';};
  section.querySelector('.kpis').innerHTML=[
    kpi('Bets',h.Total,'int'),kpi('Wins',h.Win,'int'),kpi('Losses',h.Loss,'int'),kpi('Pushes',h.Push,'int'),kpi('Win %',h.Win_Pct,'pct'),kpi('Units',h.units,'signed'),kpi('ROI excl. pushes',h.ROI_Excluding_Pushes,'pct'),kpi('ROI incl. pushes',h.ROI_Including_Pushes,'pct'),kpi('Avg EV',h.avg_ev,'pct'),kpi('Avg odds',h.avg_odds,'num')
  ].join('');

  renderTable(data.by_market_summary||[],[
    {key:'market_type',label:'Market'},...OVERVIEW_COLUMNS
  ],section.querySelector('.by-market-summary'));

  ['moneyline','run_line','total'].forEach(mt=>{
    const wrap=section.querySelector('.panel-'+mt);const md=(data.markets&&data.markets[mt])||{by:{},by_side:{}};const dims=Object.keys(md.by||{});
    wrap.innerHTML='<div class="controls"><label>Dimension: <select class="dim-select">'+dims.map(d=>'<option value="'+d+'">'+d.replaceAll('_',' ')+'</option>').join('')+'</select></label><label>View: <select class="view-select"><option value="overall">Overall</option><option value="side">Split by side</option></select></label></div><div class="market-table"></div>';
    const dimSel=wrap.querySelector('.dim-select'),viewSel=wrap.querySelector('.view-select'),target=wrap.querySelector('.market-table');
    function refresh(){const dim=dimSel.value;const side=viewSel.value==='side';renderTable(side?(md.by_side[dim]||[]):(md.by[dim]||[]),side?METRIC_SIDE_COLUMNS:METRIC_COLUMNS,target);}
    dimSel.onchange=refresh;viewSel.onchange=refresh;refresh();
  });

  const marketArea=section.querySelector('.market-area');marketArea.querySelectorAll(':scope > .tabs .tab').forEach(t=>t.onclick=()=>showTab(marketArea,t.dataset.key));

  const ov=data.overview||{};
  renderTable(ov.by_market||[],[{key:'variable',label:'Market'},...OVERVIEW_COLUMNS],section.querySelector('.overview-market'));
  renderTable(ov.by_side_group||[],[{key:'variable',label:'Side'},...OVERVIEW_COLUMNS],section.querySelector('.overview-side'));
  renderTable(ov.by_date||[],[{key:'variable',label:'Date'},...OVERVIEW_COLUMNS,{key:'cumulative_units',label:'Cumulative units',fmt:'num',decimals:2,color:true}],section.querySelector('.overview-date'));
  renderTable(ov.by_day_night||[],[{key:'variable',label:'Day / Night'},...OVERVIEW_COLUMNS],section.querySelector('.overview-day-night'));
  renderTable(ov.by_low_confidence||[],[{key:'variable',label:'Low confidence'},...OVERVIEW_COLUMNS],section.querySelector('.overview-confidence'));
  const overviewArea=section.querySelector('.overview-area');overviewArea.querySelectorAll(':scope > .tabs .tab').forEach(t=>t.onclick=()=>showTab(overviewArea,t.dataset.key));
}
"""


def feed_section_html(feed_key: str) -> str:
    cfg = FEEDS[feed_key]
    return f"""
<section class="feed-section" data-feed="{html.escape(feed_key)}">
  <h2>{html.escape(cfg['label'])} Analytics</h2>
  <div class="kpis"></div>

  <h2>By Market</h2>
  <div class="by-market-summary"></div>

  <h2>Per Market Drilldown</h2>
  <div class="market-area">
    <div class="tabs">
      <div class="tab active" data-key="moneyline">Moneyline</div>
      <div class="tab" data-key="run_line">Run Line</div>
      <div class="tab" data-key="total">Total</div>
    </div>
    <div class="tab-body">
      <div class="tab-panel panel-moneyline" data-key="moneyline"></div>
      <div class="tab-panel panel-run_line" data-key="run_line" style="display:none"></div>
      <div class="tab-panel panel-total" data-key="total" style="display:none"></div>
    </div>
  </div>

  <h2>Overview</h2>
  <div class="overview-area">
    <div class="tabs">
      <div class="tab active" data-key="market">By market</div>
      <div class="tab" data-key="side">By side</div>
      <div class="tab" data-key="date">By date</div>
      <div class="tab" data-key="day_night">Day / night</div>
      <div class="tab" data-key="confidence">Low confidence</div>
    </div>
    <div class="tab-body">
      <div class="tab-panel overview-market" data-key="market"></div>
      <div class="tab-panel overview-side" data-key="side" style="display:none"></div>
      <div class="tab-panel overview-date" data-key="date" style="display:none"></div>
      <div class="tab-panel overview-day-night" data-key="day_night" style="display:none"></div>
      <div class="tab-panel overview-confidence" data-key="confidence" style="display:none"></div>
    </div>
  </div>
</section>
"""


def build_dashboard() -> str:
    built_at = datetime.now(UTC).isoformat(timespec="seconds")
    payload = {feed_key: collect_feed_data(feed_key) for feed_key in FEEDS}
    payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)

    buttons = "\n".join(
        f'<button class="feed-btn{" active" if idx == 0 else ""}" data-feed="{feed_key}" '
        f'onclick="selectFeed(\'{feed_key}\')">{html.escape(cfg["label"])}</button>'
        for idx, (feed_key, cfg) in enumerate(FEEDS.items())
    )
    sections = "\n".join(feed_section_html(feed_key) for feed_key in FEEDS)
    first_feed = next(iter(FEEDS))

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Baseball Dashboard</title>
<link rel="stylesheet" href="assets/css/matstheme.css">
<style>{CSS}</style>
</head>
<body>
<div id="nav-placeholder"></div>
<header>
  <h1>Baseball Dashboard</h1>
  <span class="ts">Built {html.escape(built_at)} UTC</span>
</header>
<div class="feed-bar">
  <span class="lbl">Feed:</span>
  {buttons}
</div>
<main>{sections}</main>
<script src="assets/js/shared/nav.js"></script>
<script>
{JS}
const ALL_DATA={payload_json};
document.addEventListener('DOMContentLoaded',()=>{{
  Object.keys(ALL_DATA).forEach(feed=>buildFeedSection(feed,ALL_DATA[feed]));
  let initial='{first_feed}';
  try{{const stored=localStorage.getItem('baseball_dash_feed');if(stored&&ALL_DATA[stored])initial=stored;}}catch(e){{}}
  selectFeed(initial);
}});
</script>
</body>
</html>"""


def run() -> None:
    # ANALYTICS_MULTI_OUTPUT
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    master_page = build_dashboard()
    league_page = (
        master_page
        .replace("<title>Baseball Dashboard</title>", "<title>MLB Dashboard</title>", 1)
        .replace("<h1>Baseball Dashboard</h1>", "<h1>MLB Dashboard</h1>", 1)
    )

    for output_path, page in (
        (OUTPUT_FILE, master_page),
        (LEAGUE_OUTPUT_FILE, league_page),
    ):
        output_path.write_text(page, encoding="utf-8")
        rows = page.count("\n") + 1
        bytes_written = len(page.encode("utf-8"))
        log_output(output_path, rows, bytes_written)
        log("INFO", f"dashboard -> {output_path}")


def main() -> None:
    status = "FAILED"
    try:
        run()
        status = "SUCCESS"
    except Exception as exc:
        error(f"Unhandled exception: {type(exc).__name__}: {exc}")
        trace = traceback.format_exc()
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(trace)
            if not trace.endswith("\n"):
                f.write("\n")
        raise
    finally:
        finish(status)


if __name__ == "__main__":
    main()

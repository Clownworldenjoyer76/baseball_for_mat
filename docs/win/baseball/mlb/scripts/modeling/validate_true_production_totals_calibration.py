#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import re
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.special import expit
from scipy.stats import poisson

BASE = Path("docs/win/baseball/mlb")
PRED = BASE / "modeling/probability_calibration/calibration_predictions.csv"
DIST = BASE / "modeling/count_distribution_backtest/game_probabilities.csv"
ART = BASE / "models/probability_calibration/market_calibrators.json"
OUT = BASE / "modeling/production_totals_calibration_validation"

BUCKETS=[(0.30,0.40),(0.40,0.50),(0.50,0.60),(0.60,0.70),(0.70,0.80)]
BOOT=10000
SEED=1414
EPS=1e-8


def root():
    for start in [Path.cwd().resolve(), Path(__file__).resolve().parent]:
        for p in (start,*start.parents):
            if (p/BASE).is_dir():
                return p
    raise RuntimeError("repo root not found")


def parse_line(contract):
    vals=re.findall(r"(?<!\d)(\d+(?:\.\d+)?)(?!\d)",str(contract))
    if not vals:
        raise RuntimeError(f"cannot parse line: {contract}")
    return float(vals[-1])


def side(contract):
    return "under" if "under" in str(contract).lower() else "over"


def ece(y,p):
    y=np.asarray(y,float); p=np.asarray(p,float)
    edges=np.linspace(0,1,11)
    ids=np.digitize(np.clip(p,0,1),edges[1:-1],right=False)
    out=0.0
    for i in range(10):
        m=ids==i
        if m.any():
            out += float(m.mean())*abs(float(p[m].mean())-float(y[m].mean()))
    return float(out)


def log_loss(y,p):
    y=np.asarray(y,float); p=np.clip(np.asarray(p,float),EPS,1-EPS)
    return float(np.mean(-(y*np.log(p)+(1-y)*np.log(1-p))))


def ci_gap(p,y,seed):
    d=np.asarray(p,float)-np.asarray(y,float)
    rng=np.random.default_rng(seed)
    n=len(d)
    vals=np.empty(BOOT,float)
    pos=0
    while pos<BOOT:
        k=min(500,BOOT-pos)
        idx=rng.integers(0,n,size=(k,n))
        vals[pos:pos+k]=d[idx].mean(axis=1)
        pos+=k
    lo,hi=np.quantile(vals,[.025,.975])
    return float(lo),float(hi)


def raw_over_conditional(mu,line):
    frac=abs(line-round(line))
    if frac<1e-9:
        k=int(round(line))
        u=float(poisson.cdf(k-1,mu))
        o=float(1-poisson.cdf(k,mu))
    elif abs(frac-.5)<1e-9:
        k=int(math.floor(line))
        u=float(poisson.cdf(k,mu))
        o=float(1-u)
    else:
        raise RuntimeError(f"unsupported total line {line}")
    return o/(o+u)


def main():
    r=root()

    pred=pd.read_csv(r/PRED)
    pred=pred[
        pred["market"].eq("total")
        & pred["period"].isin(["cv_fold_2","cv_fold_3","cv_fold_4"])
    ].copy()
    pred["line"]=pred["contract"].map(parse_line)
    pred["side"]=pred["contract"].map(side)
    pred["y"]=pd.to_numeric(pred["y"],errors="raise")

    dist=pd.read_csv(r/DIST)
    dist=dist[
        dist["distribution"].eq("poisson_skellam")
        & dist["period"].isin(["cv_fold_2","cv_fold_3","cv_fold_4"])
    ][["period","game_id","mean_home_runs","mean_away_runs"]].copy()

    pred=pred.merge(dist,on=["period","game_id"],how="left",validate="many_to_one")
    if pred[["mean_home_runs","mean_away_runs"]].isna().any().any():
        raise RuntimeError("missing run means")

    obj=json.loads((r/ART).read_text(encoding="utf-8"))
    total=obj["markets"]["total"]
    a=float(total["a"]); b=float(total["b"]); c=float(total["intercept"])

    probs=[]
    for x in pred.itertuples(index=False):
        q=raw_over_conditional(
            float(x.mean_home_runs)+float(x.mean_away_runs),
            float(x.line),
        )
        q=np.clip(q,EPS,1-EPS)
        over=float(expit(c+a*math.log(q)-b*math.log1p(-q)))
        probs.append(over if x.side=="over" else 1-over)

    pred["production_p"]=probs

    overall=pd.DataFrame([{
        "rows":len(pred),
        "log_loss":log_loss(pred["y"],pred["production_p"]),
        "ece":ece(pred["y"],pred["production_p"]),
    }])

    rows=[]
    for i,(lo,hi) in enumerate(BUCKETS):
        s=pred[(pred["production_p"]>=lo)&(pred["production_p"]<hi)]
        if s.empty:
            continue
        gap=float(s["production_p"].mean()-s["y"].mean())
        clo,chi=ci_gap(s["production_p"],s["y"],SEED+i)
        rows.append({
            "bucket":f"{lo:.1f}-{hi:.1f}",
            "rows":len(s),
            "mean_pred":float(s["production_p"].mean()),
            "observed_rate":float(s["y"].mean()),
            "gap":gap,
            "ci95_low":clo,
            "ci95_high":chi,
            "confirmed_overstatement":bool(clo>0),
            "confirmed_understatement":bool(chi<0),
        })

    buckets=pd.DataFrame(rows)

    out=r/OUT
    out.mkdir(parents=True,exist_ok=True)
    overall.to_csv(out/"overall_metrics.csv",index=False)
    buckets.to_csv(out/"bucket_metrics.csv",index=False)

    print("TRUE PRODUCTION TOTALS CALIBRATION")
    print(overall.to_string(index=False))
    print(buckets.to_string(index=False))
    print("CONFIRMED BIASED BUCKETS:", int((buckets["confirmed_overstatement"]|buckets["confirmed_understatement"]).sum()))


if __name__=="__main__":
    main()

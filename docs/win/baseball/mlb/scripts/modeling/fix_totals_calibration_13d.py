#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import re
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.special import expit
from scipy.stats import poisson

BASE = Path("docs/win/baseball/mlb")
PREDICTIONS = BASE / "modeling/probability_calibration/calibration_predictions.csv"
DIST_PROBS = BASE / "modeling/count_distribution_backtest/game_probabilities.csv"
ARTIFACT = BASE / "models/probability_calibration/market_calibrators.json"
OUTPUT_DIR = BASE / "modeling/totals_calibration_13d"

TARGET_BUCKETS = [(0.30,0.40),(0.40,0.50),(0.50,0.60),(0.60,0.70),(0.70,0.80)]
ECE_EDGES = np.linspace(0,1,11)
MIN_P = 1e-8
TOL = 1e-12
BOOTSTRAP_DRAWS = 10000
SEED = 1314


def repo_root():
    for start in [Path.cwd().resolve(), Path(__file__).resolve().parent]:
        for p in (start,*start.parents):
            if (p/BASE).is_dir():
                return p
    raise RuntimeError("Could not locate repo root")


def resolve(root,p):
    return p if p.is_absolute() else root/p


def clip(p):
    return np.clip(np.asarray(p,float),MIN_P,1-MIN_P)


def log_loss(y,p,w=None):
    y=np.asarray(y,float); p=clip(p)
    loss=-(y*np.log(p)+(1-y)*np.log(1-p))
    if w is None:
        return float(loss.mean())
    w=np.asarray(w,float)
    return float(np.average(loss,weights=w))


def ece(y,p):
    y=np.asarray(y,float); p=np.asarray(p,float)
    ids=np.digitize(np.clip(p,0,1),ECE_EDGES[1:-1],right=False)
    out=0.0
    for i in range(10):
        m=ids==i
        if m.any():
            out += float(m.mean())*abs(float(p[m].mean())-float(y[m].mean()))
    return float(out)


def bootstrap_ci(p,y,seed):
    d=np.asarray(p,float)-np.asarray(y,float)
    n=len(d)
    rng=np.random.default_rng(seed)
    vals=np.empty(BOOTSTRAP_DRAWS,float)
    pos=0
    while pos<BOOTSTRAP_DRAWS:
        k=min(500,BOOTSTRAP_DRAWS-pos)
        idx=rng.integers(0,n,size=(k,n))
        vals[pos:pos+k]=d[idx].mean(axis=1)
        pos+=k
    lo,hi=np.quantile(vals,[.025,.975])
    return float(lo),float(hi)


def fold_num(s):
    m=re.fullmatch(r"cv_fold_(\d+)",str(s))
    if not m:
        raise ValueError(f"Bad fold label: {s}")
    return int(m.group(1))


def parse_line(contract):
    vals=re.findall(r"(?<!\d)(\d+(?:\.\d+)?)(?!\d)",str(contract))
    if not vals:
        raise ValueError(f"Cannot parse total line from {contract!r}")
    return float(vals[-1])


def contract_side(contract):
    return "under" if "under" in str(contract).lower() else "over"


def load_predictions(path):
    df=pd.read_csv(path)
    need={"market","period","game_date","game_id","contract","y","raw_p","calibrated_p"}
    miss=need-set(df.columns)
    if miss:
        raise RuntimeError(f"Missing prediction columns: {sorted(miss)}")
    df=df[df["market"].eq("total")].copy()
    df["game_date"]=pd.to_datetime(df["game_date"],errors="raise")
    df["line"]=df["contract"].map(parse_line)
    for c in ("y","raw_p","calibrated_p"):
        df[c]=pd.to_numeric(df[c],errors="coerce")
    if df[["y","raw_p","calibrated_p"]].isna().any().any():
        raise RuntimeError("NaN in total predictions")
    return df.reset_index(drop=True)


def load_dist(path):
    df=pd.read_csv(path)
    need={"period","distribution","game_date","game_id","actual_home_runs","actual_away_runs","mean_home_runs","mean_away_runs"}
    miss=need-set(df.columns)
    if miss:
        raise RuntimeError(f"Missing item6 columns: {sorted(miss)}")
    df=df[df["distribution"].eq("poisson_skellam")].copy()
    df["game_date"]=pd.to_datetime(df["game_date"],errors="raise")
    return df.reset_index(drop=True)


def raw_total(mu,line):
    frac=abs(line-round(line))
    if frac<1e-9:
        k=int(round(line))
        under=float(poisson.cdf(k-1,mu))
        over=float(1-poisson.cdf(k,mu))
    elif abs(frac-.5)<1e-9:
        k=int(math.floor(line))
        under=float(poisson.cdf(k,mu))
        over=float(1-under)
    else:
        raise ValueError(f"Unsupported total line {line}")
    return over,under


def reconstruct_fold1(pred,dist):
    games=dist[dist["period"].eq("cv_fold_1")].copy()
    contracts=sorted(pred["contract"].astype(str).unique())
    rows=[]
    for g in games.itertuples(index=False):
        mu=float(g.mean_home_runs)+float(g.mean_away_runs)
        actual=float(g.actual_home_runs)+float(g.actual_away_runs)
        for contract in contracts:
            line=parse_line(contract)
            side=contract_side(contract)
            po,pu=raw_total(mu,line)
            if abs(actual-line)<1e-9:
                continue
            resolved=po+pu
            raw_over=po/resolved
            raw=raw_over if side=="over" else 1-raw_over
            y=float(actual>line) if side=="over" else float(actual<line)
            rows.append({
                "period":"cv_fold_1","game_date":g.game_date,"game_id":g.game_id,
                "contract":contract,"line":line,"y":y,"raw_p":raw
            })
    out=pd.DataFrame(rows)
    if out.empty:
        raise RuntimeError("No fold1 rows reconstructed")
    return out


@dataclass(frozen=True)
class Spec:
    family:str
    half_life:int | None
    window:int | None
    ridge:float

    @property
    def name(self):
        h="all" if self.half_life is None else str(self.half_life)
        w="all" if self.window is None else str(self.window)
        return f"{self.family}_hl{h}_win{w}_r{self.ridge:g}"


def specs():
    out=[]
    for family in ("beta","platt","intercept"):
        for half_life in (None,14,30,60,120):
            for window in (None,30,60,90):
                for ridge in (0.0,0.01,0.1):
                    out.append(Spec(family,half_life,window,ridge))
    return out


def training_weights(dates,reference,spec):
    dates=pd.to_datetime(dates)
    age=(pd.Timestamp(reference)-dates).dt.days.to_numpy(float)
    if np.any(age<0):
        raise RuntimeError("Training data occurs after validation reference date")
    w=np.ones(len(age),float)
    if spec.window is not None:
        w=np.where(age<=spec.window,1.0,0.0)
    if spec.half_life is not None:
        w=w*np.exp(-math.log(2.0)*age/spec.half_life)
    return w


def predict_beta(p,params):
    p=clip(p)
    a,b,c=params
    return expit(c+a*np.log(p)-b*np.log1p(-p))


def fit_calibrator(train,reference,spec):
    p=train["raw_p"].to_numpy(float)
    y=train["y"].to_numpy(float)
    w=training_weights(train["game_date"],reference,spec)
    keep=w>0
    p=p[keep]; y=y[keep]; w=w[keep]
    if len(p)<100:
        raise RuntimeError(f"{spec.name}: only {len(p)} weighted training rows")

    lp=np.log(clip(p))
    lq=np.log1p(-clip(p))
    z=np.log(clip(p)/(1-clip(p)))

    if spec.family=="beta":
        x0=np.array([math.log(.7),math.log(.7),0.0],float)
        def obj(t):
            a=math.exp(t[0]); b=math.exp(t[1]); c=t[2]
            q=expit(c+a*lp-b*lq)
            penalty=spec.ridge*(t[0]**2+t[1]**2+c**2)
            return log_loss(y,q,w)+penalty
        res=minimize(obj,x0,method="L-BFGS-B",bounds=[(-5,5),(-5,5),(-10,10)])
        if not res.success:
            raise RuntimeError(f"{spec.name} fit failed: {res.message}")
        return (float(math.exp(res.x[0])),float(math.exp(res.x[1])),float(res.x[2]))

    if spec.family=="platt":
        x0=np.array([math.log(.7),0.0],float)
        def obj(t):
            s=math.exp(t[0]); c=t[1]
            q=expit(c+s*z)
            penalty=spec.ridge*(t[0]**2+c**2)
            return log_loss(y,q,w)+penalty
        res=minimize(obj,x0,method="L-BFGS-B",bounds=[(-5,5),(-10,10)])
        if not res.success:
            raise RuntimeError(f"{spec.name} fit failed: {res.message}")
        s=float(math.exp(res.x[0])); c=float(res.x[1])
        return (s,s,c)

    if spec.family=="intercept":
        x0=np.array([0.0],float)
        def obj(t):
            c=t[0]
            q=expit(c+z)
            penalty=spec.ridge*c**2
            return log_loss(y,q,w)+penalty
        res=minimize(obj,x0,method="L-BFGS-B",bounds=[(-10,10)])
        if not res.success:
            raise RuntimeError(f"{spec.name} fit failed: {res.message}")
        return (1.0,1.0,float(res.x[0]))

    raise ValueError(spec.family)


def chronological_predictions(all_cv,eval_rows,spec):
    outs=[]
    for period in ("cv_fold_2","cv_fold_3","cv_fold_4"):
        f=fold_num(period)
        tr=all_cv[all_cv["period"].map(fold_num)<f].copy()
        va=eval_rows[eval_rows["period"].eq(period)].copy()
        reference=va["game_date"].min()
        params=fit_calibrator(tr,reference,spec)
        va["candidate_p"]=predict_beta(va["raw_p"].to_numpy(),params)
        outs.append(va)
    return pd.concat(outs,ignore_index=True)


def target_metrics(df,name,bootstrap):
    rows=[]
    bp=df["calibrated_p"].to_numpy(float)
    for i,(lo,hi) in enumerate(TARGET_BUCKETS):
        m=(bp>=lo)&(bp<hi)
        s=df.loc[m]
        if len(s)<30:
            raise RuntimeError(f"Target bucket {lo}-{hi} has only {len(s)} rows")
        y=s["y"].to_numpy(float)
        b=s["calibrated_p"].to_numpy(float)
        c=s["candidate_p"].to_numpy(float)
        bg=float(b.mean()-y.mean())
        cg=float(c.mean()-y.mean())
        clo=chi=float("nan")
        if bootstrap:
            clo,chi=bootstrap_ci(c,y,SEED+i+sum(map(ord,name)))
        rows.append({
            "candidate":name,
            "bucket":f"{lo:.1f}-{hi:.1f}",
            "rows":len(s),
            "baseline_mean_pred":float(b.mean()),
            "candidate_mean_pred":float(c.mean()),
            "observed_rate":float(y.mean()),
            "baseline_gap":bg,
            "candidate_gap":cg,
            "baseline_abs_error":abs(bg),
            "candidate_abs_error":abs(cg),
            "abs_error_improved":bool(abs(cg)<abs(bg)-TOL),
            "candidate_ci95_low":clo,
            "candidate_ci95_high":chi,
            "confirmed_overstatement_removed":bool(clo<=TOL) if bootstrap else False,
        })
    return pd.DataFrame(rows)


def evaluate(df,name,bootstrap=False):
    y=df["y"].to_numpy(float)
    b=df["calibrated_p"].to_numpy(float)
    c=df["candidate_p"].to_numpy(float)
    buckets=target_metrics(df,name,bootstrap)
    row={
        "candidate":name,
        "rows":len(df),
        "baseline_ll":log_loss(y,b),
        "candidate_ll":log_loss(y,c),
        "baseline_ece":ece(y,b),
        "candidate_ece":ece(y,c),
        "all_bucket_abs_errors_improved":bool(buckets["abs_error_improved"].all()),
        "ll_not_worse":bool(log_loss(y,c)<=log_loss(y,b)+TOL),
        "ece_not_worse":bool(ece(y,c)<=ece(y,b)+TOL),
    }
    if bootstrap:
        row["all_confirmed_overstatements_removed"]=bool(buckets["confirmed_overstatement_removed"].all())
        row["gate_passed"]=bool(
            row["all_bucket_abs_errors_improved"] and
            row["all_confirmed_overstatements_removed"] and
            row["ll_not_worse"] and
            row["ece_not_worse"]
        )
    return row,buckets


def update_artifact(path,params,spec,fit_rows):
    obj=json.loads(path.read_text(encoding="utf-8"))
    total=obj["markets"]["total"]
    a,b,c=params
    total["a"]=float(a)
    total["b"]=float(b)
    total["intercept"]=float(c)
    total["log_a"]=float(math.log(a))
    total["log_b"]=float(math.log(b))
    total["method"]="beta_logistic"
    total["enabled"]=True
    total["fit_observations"]=int(fit_rows)
    total["observations"]=int(fit_rows)
    total["item13d_family"]=spec.family
    total["item13d_half_life_days"]=spec.half_life
    total["item13d_window_days"]=spec.window
    total["item13d_ridge"]=spec.ridge
    total["item13d_final_test_used_for_fit_or_selection"]=False
    path.write_text(json.dumps(obj,indent=2)+"\n",encoding="utf-8")


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--no-promote",action="store_true")
    args=ap.parse_args()

    root=repo_root()
    pred=load_predictions(resolve(root,PREDICTIONS))
    dist=load_dist(resolve(root,DIST_PROBS))
    fold1=reconstruct_fold1(pred,dist)

    cv_existing=pred[pred["period"].isin(["cv_fold_2","cv_fold_3","cv_fold_4"])].copy()
    all_cv=pd.concat([
        fold1,
        pred[pred["period"].isin(["cv_fold_2","cv_fold_3","cv_fold_4"])][
            ["period","game_date","game_id","contract","line","y","raw_p"]
        ].copy()
    ],ignore_index=True)

    prelim=[]
    cache={}
    spec_map={}

    for spec in specs():
        try:
            df=chronological_predictions(all_cv,cv_existing,spec)
        except RuntimeError:
            continue
        row,_=evaluate(df,spec.name,False)
        prelim.append(row)
        cache[spec.name]=df
        spec_map[spec.name]=spec

    if not prelim:
        raise RuntimeError("No candidate completed")

    predf=pd.DataFrame(prelim).sort_values(["candidate_ll","candidate_ece"])
    promising=predf[
        predf["all_bucket_abs_errors_improved"] &
        predf["ll_not_worse"] &
        predf["ece_not_worse"]
    ].copy()

    gates=[]
    bucket_frames=[]
    for name in promising["candidate"].tolist():
        row,buckets=evaluate(cache[name],name,True)
        gates.append(row)
        bucket_frames.append(buckets)

    passing=[r for r in gates if r["gate_passed"]]
    selected={"status":"no_candidate_passed","production_changed":False}
    selected_params=None
    selected_spec=None

    if passing:
        best=sorted(passing,key=lambda r:(r["candidate_ll"],r["candidate_ece"]))[0]
        selected_spec=spec_map[best["candidate"]]

        # Final production fit uses all four CV OOS periods only.
        reference=all_cv["game_date"].max()+pd.Timedelta(days=1)
        selected_params=fit_calibrator(all_cv,reference,selected_spec)

        selected={
            "status":"candidate_passed",
            "candidate":best,
            "production_changed":False,
            "fit_params":{
                "a":selected_params[0],
                "b":selected_params[1],
                "intercept":selected_params[2],
            },
            "spec":{
                "family":selected_spec.family,
                "half_life_days":selected_spec.half_life,
                "window_days":selected_spec.window,
                "ridge":selected_spec.ridge,
            },
        }

        if not args.no_promote:
            artifact=resolve(root,ARTIFACT)
            stamp=datetime.now().strftime("%Y%m%d_%H%M%S")
            backup=artifact.with_name(artifact.name+f".item13d_{stamp}.bak")
            shutil.copy2(artifact,backup)
            update_artifact(artifact,selected_params,selected_spec,len(all_cv))
            selected["production_changed"]=True
            selected["artifact_backup"]=str(backup)

    # Final-test reference only AFTER candidate selection.
    final_ref=[]
    final=pred[pred["period"].eq("final_test")].copy()
    if not final.empty:
        y=final["y"].to_numpy(float)
        base=final["calibrated_p"].to_numpy(float)
        final_ref.append({
            "system":"current_production_before_13d",
            "rows":len(final),
            "log_loss":log_loss(y,base),
            "ece":ece(y,base),
        })
        if selected_params is not None:
            q=predict_beta(final["raw_p"].to_numpy(),selected_params)
            final_ref.append({
                "system":"selected_13d_reference",
                "rows":len(final),
                "log_loss":log_loss(y,q),
                "ece":ece(y,q),
            })

    outdir=resolve(root,OUTPUT_DIR)
    outdir.mkdir(parents=True,exist_ok=True)
    predf.to_csv(outdir/"candidate_preliminary_metrics.csv",index=False)
    pd.DataFrame(gates).to_csv(outdir/"candidate_gate_metrics.csv",index=False)
    if bucket_frames:
        pd.concat(bucket_frames,ignore_index=True).to_csv(outdir/"target_bucket_metrics.csv",index=False)
    pd.DataFrame(final_ref).to_csv(outdir/"final_test_reference_metrics.csv",index=False)
    (outdir/"selected_candidate.json").write_text(json.dumps(selected,indent=2)+"\n",encoding="utf-8")

    print("ITEM 13D RESULT")
    print(json.dumps(selected,indent=2))
    if gates:
        cols=[
            "candidate","baseline_ll","candidate_ll","baseline_ece","candidate_ece",
            "all_bucket_abs_errors_improved","all_confirmed_overstatements_removed",
            "ll_not_worse","ece_not_worse","gate_passed"
        ]
        print(pd.DataFrame(gates)[cols].to_string(index=False))
    else:
        print("No candidate reached the bootstrap gate.")
    print("FINAL TEST REFERENCE ONLY")
    print(pd.DataFrame(final_ref).to_string(index=False) if final_ref else "No final_test rows")
    print("PRODUCTION CHANGED:",selected.get("production_changed",False))


if __name__=="__main__":
    main()

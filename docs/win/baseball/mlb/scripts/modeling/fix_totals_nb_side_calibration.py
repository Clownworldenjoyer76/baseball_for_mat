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
from scipy.stats import nbinom
from sklearn.isotonic import IsotonicRegression

BASE = Path("docs/win/baseball/mlb")
PREDICTIONS = BASE / "modeling/probability_calibration/calibration_predictions.csv"
DIST_PROBS = BASE / "modeling/count_distribution_backtest/game_probabilities.csv"
CAL_ARTIFACT = BASE / "models/probability_calibration/market_calibrators.json"
OUTDIR = BASE / "modeling/totals_nb_side_calibration"
NB_ARTIFACT = BASE / "models/probability_calibration/totals_nb_model.json"
NB_HELPER = BASE / "scripts/01_merge/totals_nb_model.py"
BUILD_JUICE = BASE / "scripts/01_merge/build_juice_files.py"

TARGET_BUCKETS = [(0.30,0.40),(0.40,0.50),(0.50,0.60),(0.60,0.70),(0.70,0.80)]
ECE_EDGES = np.linspace(0.0,1.0,11)
MIN_P = 1e-8
TOL = 1e-12
BOOTSTRAP_DRAWS = 10000
SEED = 1409

CV_ALPHAS = {
    "cv_fold_1": (0.039616, 0.317081),
    "cv_fold_2": (0.038113, 0.312060),
    "cv_fold_3": (0.044312, 0.306334),
    "cv_fold_4": (0.047793, 0.309868),
}
PRODUCTION_ALPHA_HOME = 0.30489153
PRODUCTION_ALPHA_AWAY = 0.31311742

PATCH_IMPORT = "from totals_nb_model import totals_nb_candidate_probabilities"
PATCH_RETURN = "\n".join([
    "    return totals_nb_candidate_probabilities(",
    "        model_home_runs,",
    "        model_away_runs,",
    "        total_line,",
    "    )",
])


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


def log_loss(y,p):
    y=np.asarray(y,float); p=clip(p)
    return float(np.mean(-(y*np.log(p)+(1-y)*np.log(1-p))))


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


def parse_line(contract):
    vals=re.findall(r"(?<!\d)(\d+(?:\.\d+)?)(?!\d)",str(contract))
    if not vals:
        raise RuntimeError(f"Could not parse line from {contract!r}")
    return float(vals[-1])


def contract_side(contract):
    return "under" if "under" in str(contract).lower() else "over"


def fold_num(s):
    m=re.fullmatch(r"cv_fold_(\d+)",str(s))
    if not m:
        raise RuntimeError(f"Bad fold {s}")
    return int(m.group(1))


def load_inputs(root):
    pred=pd.read_csv(resolve(root,PREDICTIONS))
    need={"market","period","game_date","game_id","contract","y","calibrated_p"}
    miss=need-set(pred.columns)
    if miss:
        raise RuntimeError(f"Missing calibration columns: {sorted(miss)}")
    pred=pred[pred["market"].eq("total")].copy()
    pred["game_date"]=pd.to_datetime(pred["game_date"],errors="raise")
    pred["line"]=pred["contract"].map(parse_line)
    pred["side"]=pred["contract"].map(contract_side)
    pred["y"]=pd.to_numeric(pred["y"],errors="raise")
    pred["calibrated_p"]=pd.to_numeric(pred["calibrated_p"],errors="raise")

    dist=pd.read_csv(resolve(root,DIST_PROBS))
    need2={"period","distribution","game_date","game_id","actual_home_runs","actual_away_runs","mean_home_runs","mean_away_runs"}
    miss2=need2-set(dist.columns)
    if miss2:
        raise RuntimeError(f"Missing item #6 columns: {sorted(miss2)}")
    dist=dist[dist["distribution"].eq("poisson_skellam")].copy()
    dist["game_date"]=pd.to_datetime(dist["game_date"],errors="raise")
    return pred.reset_index(drop=True),dist.reset_index(drop=True)


def current_total_beta(path):
    obj=json.loads(path.read_text(encoding="utf-8"))
    total=obj["markets"]["total"]
    return {
        "a":float(total["a"]),
        "b":float(total["b"]),
        "intercept":float(total["intercept"]),
    }


def nb_pmf(mu,alpha):
    if mu<0 or alpha<=0:
        raise RuntimeError(f"Invalid NB params mu={mu} alpha={alpha}")
    r=1.0/alpha
    prob=r/(r+mu)
    hi=int(max(30,math.ceil(float(nbinom.ppf(1-1e-12,r,prob)))))
    hi=min(hi,250)
    k=np.arange(hi+1)
    pmf=nbinom.pmf(k,r,prob)
    if not np.all(np.isfinite(pmf)):
        raise RuntimeError("Non-finite NB pmf")
    return pmf


def nb_total_probs(mu_h,mu_a,line,alpha_h,alpha_a):
    ph=nb_pmf(float(mu_h),float(alpha_h))
    pa=nb_pmf(float(mu_a),float(alpha_a))
    pt=np.convolve(ph,pa)
    mass=float(pt.sum())
    if mass<=0:
        raise RuntimeError("Invalid NB total mass")
    pt=pt/mass

    frac=abs(float(line)-round(float(line)))
    if frac<1e-9:
        k=int(round(float(line)))
        p_under=float(pt[:k].sum())
        p_push=float(pt[k]) if k<len(pt) else 0.0
        p_over=float(pt[k+1:].sum()) if k+1<len(pt) else 0.0
    elif abs(frac-.5)<1e-9:
        k=int(math.floor(float(line)))
        p_under=float(pt[:k+1].sum())
        p_push=0.0
        p_over=float(pt[k+1:].sum()) if k+1<len(pt) else 0.0
    else:
        raise RuntimeError(f"Unsupported total line {line}")

    s=p_under+p_push+p_over
    if abs(s-1.0)>1e-8:
        p_under/=s; p_push/=s; p_over/=s
    return p_over,p_under,p_push


def build_nb_over_rows(dist,lines,periods):
    rows=[]
    for g in dist[dist["period"].isin(periods)].itertuples(index=False):
        if g.period in CV_ALPHAS:
            ah,aa=CV_ALPHAS[g.period]
        elif g.period=="final_test":
            ah,aa=PRODUCTION_ALPHA_HOME,PRODUCTION_ALPHA_AWAY
        else:
            continue
        actual=float(g.actual_home_runs)+float(g.actual_away_runs)
        for line in lines:
            if abs(actual-line)<1e-9:
                continue
            po,pu,_=nb_total_probs(g.mean_home_runs,g.mean_away_runs,line,ah,aa)
            rows.append({
                "period":g.period,
                "game_date":g.game_date,
                "game_id":g.game_id,
                "line":float(line),
                "nb_over_p":float(po/(po+pu)),
                "y_over":float(actual>line),
            })
    out=pd.DataFrame(rows)
    if out.empty:
        raise RuntimeError("No NB rows built")
    return out


@dataclass(frozen=True)
class Spec:
    family:str
    ridge:float
    half_life:int|None
    blend:float

    @property
    def name(self):
        h="all" if self.half_life is None else str(self.half_life)
        return f"{self.family}_r{self.ridge:g}_hl{h}_blend{self.blend:g}"


def specs():
    out=[]
    for blend in (0.25,0.5,0.75,1.0,1.25,1.5):
        out.append(Spec("identity",0.0,None,blend))
    for half_life in (None,30,60,120):
        for blend in (0.25,0.5,0.75,1.0,1.25,1.5):
            out.append(Spec("isotonic",0.0,half_life,blend))
    for family in ("platt","beta"):
        for ridge in (0.0,0.01,0.1):
            for half_life in (None,30,60,120):
                for blend in (0.25,0.5,0.75,1.0,1.25):
                    out.append(Spec(family,ridge,half_life,blend))
    return out


def training_weights(dates,reference,half_life):
    age=(pd.Timestamp(reference)-pd.to_datetime(dates)).dt.days.to_numpy(float)
    if np.any(age<0):
        raise RuntimeError("Training data after validation reference")
    if half_life is None:
        return np.ones(len(age),float)
    return np.exp(-math.log(2.0)*age/float(half_life))


def fit_nb_calibrator(train,reference,spec):
    if spec.family=="identity":
        return {"family":"identity"}

    p=clip(train["nb_over_p"].to_numpy(float))
    y=train["y_over"].to_numpy(float)
    w=training_weights(train["game_date"],reference,spec.half_life)
    z=np.log(p/(1-p))

    if spec.family=="isotonic":
        iso=IsotonicRegression(increasing=True,out_of_bounds="clip")
        iso.fit(p,y,sample_weight=w)
        return {
            "family":"isotonic",
            "x":iso.X_thresholds_.astype(float).tolist(),
            "y":iso.y_thresholds_.astype(float).tolist(),
        }

    if spec.family=="platt":
        def obj(t):
            slope=math.exp(t[0]); c=t[1]
            q=expit(c+slope*z)
            loss=np.average(-(y*np.log(clip(q))+(1-y)*np.log1p(-clip(q))),weights=w)
            return float(loss+spec.ridge*(t[0]**2+c**2))
        res=minimize(obj,[0.0,0.0],method="L-BFGS-B",bounds=[(-5,5),(-10,10)])
        if not res.success:
            raise RuntimeError(res.message)
        return {"family":"platt","slope":float(math.exp(res.x[0])),"intercept":float(res.x[1])}

    if spec.family=="beta":
        lp=np.log(p); lq=np.log1p(-p)
        def obj(t):
            a=math.exp(t[0]); b=math.exp(t[1]); c=t[2]
            q=expit(c+a*lp-b*lq)
            loss=np.average(-(y*np.log(clip(q))+(1-y)*np.log1p(-clip(q))),weights=w)
            return float(loss+spec.ridge*(t[0]**2+t[1]**2+c**2))
        res=minimize(obj,[0.0,0.0,0.0],method="L-BFGS-B",bounds=[(-5,5),(-5,5),(-10,10)])
        if not res.success:
            raise RuntimeError(res.message)
        return {
            "family":"beta",
            "a":float(math.exp(res.x[0])),
            "b":float(math.exp(res.x[1])),
            "intercept":float(res.x[2]),
        }

    raise RuntimeError(spec.family)


def apply_nb_calibrator(p,model):
    p=clip(p)
    if model["family"]=="identity":
        return p
    if model["family"]=="isotonic":
        x=np.asarray(model["x"],float)
        y=np.asarray(model["y"],float)
        return clip(np.interp(p,x,y,left=y[0],right=y[-1]))
    if model["family"]=="platt":
        z=np.log(p/(1-p))
        return expit(float(model["intercept"])+float(model["slope"])*z)
    if model["family"]=="beta":
        return expit(
            float(model["intercept"])
            + float(model["a"])*np.log(p)
            - float(model["b"])*np.log1p(-p)
        )
    raise RuntimeError(model["family"])

def fit_side_models(train,reference,spec):
    over_model=fit_nb_calibrator(train,reference,spec)
    under=train.copy()
    under["nb_over_p"]=1.0-under["nb_over_p"].to_numpy(float)
    under["y_over"]=1.0-under["y_over"].to_numpy(float)
    under_model=fit_nb_calibrator(under,reference,spec)
    return {"over":over_model,"under":under_model}


def apply_side_models(nb_over_p,models):
    p=clip(nb_over_p)
    so=clip(apply_nb_calibrator(p,models["over"]))
    su=clip(apply_nb_calibrator(1.0-p,models["under"]))
    denom=so+su
    if np.any(denom<=0):
        raise RuntimeError("Invalid side-calibration normalization")
    return clip(so/denom)


def runtime_baseline_over(mu_h,mu_a,line,old_beta):
    mu=float(mu_h)+float(mu_a)
    frac=abs(float(line)-round(float(line)))
    from scipy.stats import poisson
    if frac<1e-9:
        k=int(round(float(line)))
        pu=float(poisson.cdf(k-1,mu))
        po=float(1-poisson.cdf(k,mu))
    elif abs(frac-.5)<1e-9:
        k=int(math.floor(float(line)))
        pu=float(poisson.cdf(k,mu))
        po=float(1-pu)
    else:
        raise RuntimeError(f"Unsupported total line {line}")
    q=po/(po+pu)
    return float(
        expit(
            old_beta["intercept"]
            + old_beta["a"]*math.log(max(q,MIN_P))
            - old_beta["b"]*math.log(max(1-q,MIN_P))
        )
    )


def build_runtime_baseline_rows(dist,lines,periods,old_beta):
    rows=[]
    for g in dist[dist["period"].isin(periods)].itertuples(index=False):
        for line in lines:
            rows.append({
                "period":g.period,
                "game_id":g.game_id,
                "line":float(line),
                "baseline_over_p":runtime_baseline_over(
                    g.mean_home_runs,g.mean_away_runs,line,old_beta
                ),
            })
    return pd.DataFrame(rows)


def candidate_eval_rows(pred,nb_rows,base_rows,all_nb,spec):
    outs=[]
    for period in ("cv_fold_2","cv_fold_3","cv_fold_4"):
        f=fold_num(period)
        train=all_nb[all_nb["period"].map(fold_num)<f].copy()
        va=pred[pred["period"].eq(period)].copy()
        nbv=nb_rows[nb_rows["period"].eq(period)].copy()
        bv=base_rows[base_rows["period"].eq(period)].copy()

        models=fit_side_models(train,va["game_date"].min(),spec)
        nbv["nb_cal_over"]=apply_side_models(nbv["nb_over_p"].to_numpy(),models)

        va=va.merge(
            nbv[["period","game_id","line","nb_cal_over"]],
            on=["period","game_id","line"],how="left",validate="many_to_one"
        )
        va=va.merge(
            bv[["period","game_id","line","baseline_over_p"]],
            on=["period","game_id","line"],how="left",validate="many_to_one"
        )
        if va[["nb_cal_over","baseline_over_p"]].isna().any().any():
            raise RuntimeError(f"Missing probabilities in {period}")

        base=np.where(
            va["side"].eq("over"),
            va["baseline_over_p"],
            1.0-va["baseline_over_p"],
        )
        nbside=np.where(
            va["side"].eq("over"),
            va["nb_cal_over"],
            1.0-va["nb_cal_over"],
        )
        va["baseline_p"]=clip(base)
        va["candidate_p"]=clip(
            (1-float(spec.blend))*va["baseline_p"].to_numpy(float)
            + float(spec.blend)*nbside
        )
        outs.append(va)

    return pd.concat(outs,ignore_index=True)

def target_metrics(df,name,bootstrap=False):
    rows=[]
    bp=df["baseline_p"].to_numpy(float)
    for i,(lo,hi) in enumerate(TARGET_BUCKETS):
        s=df[(bp>=lo)&(bp<hi)]
        if len(s)<30:
            raise RuntimeError(f"Target bucket {lo}-{hi} has only {len(s)} rows")
        y=s["y"].to_numpy(float)
        b=s["baseline_p"].to_numpy(float)
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
    b=df["baseline_p"].to_numpy(float)
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
            row["all_bucket_abs_errors_improved"]
            and row["all_confirmed_overstatements_removed"]
            and row["ll_not_worse"]
            and row["ece_not_worse"]
        )
    return row,buckets


RUNTIME_HELPER = r'''#!/usr/bin/env python3
from __future__ import annotations

import json
import math
from functools import lru_cache
from pathlib import Path

import numpy as np
from scipy.special import expit
from scipy.stats import nbinom, poisson
from sklearn.isotonic import IsotonicRegression

ROOT=Path(__file__).resolve().parents[6]
ARTIFACT=ROOT/"docs/win/baseball/mlb/models/probability_calibration/totals_nb_model.json"

@lru_cache(maxsize=1)
def _load():
    return json.loads(ARTIFACT.read_text(encoding="utf-8"))

def _clip(p):
    return np.clip(np.asarray(p,float),1e-8,1-1e-8)

def _beta(p,m):
    p=_clip(p)
    return expit(float(m["intercept"])+float(m["a"])*np.log(p)-float(m["b"])*np.log1p(-p))

def _cal(p,m):
    p=_clip(p)
    if m["family"]=="identity":
        return p
    if m["family"]=="isotonic":
        x=np.asarray(m["x"],float)
        y=np.asarray(m["y"],float)
        return np.clip(np.interp(p,x,y,left=y[0],right=y[-1]),0,1)
    if m["family"]=="platt":
        z=np.log(p/(1-p))
        return expit(float(m["intercept"])+float(m["slope"])*z)
    if m["family"]=="beta":
        return expit(float(m["intercept"])+float(m["a"])*np.log(p)-float(m["b"])*np.log1p(-p))
    raise ValueError("unknown NB calibrator")

def _nb_pmf(mu,alpha):
    r=1.0/float(alpha)
    prob=r/(r+float(mu))
    hi=int(max(30,math.ceil(float(nbinom.ppf(1-1e-12,r,prob)))))
    hi=min(hi,250)
    return nbinom.pmf(np.arange(hi+1),r,prob)

def _nb_probs(mu_h,mu_a,line,ah,aa):
    pt=np.convolve(_nb_pmf(mu_h,ah),_nb_pmf(mu_a,aa))
    pt=pt/pt.sum()
    frac=abs(float(line)-round(float(line)))
    if frac<1e-9:
        k=int(round(float(line)))
        u=float(pt[:k].sum()); push=float(pt[k]) if k<len(pt) else 0.0
        o=float(pt[k+1:].sum()) if k+1<len(pt) else 0.0
    elif abs(frac-.5)<1e-9:
        k=int(math.floor(float(line)))
        u=float(pt[:k+1].sum()); push=0.0
        o=float(pt[k+1:].sum()) if k+1<len(pt) else 0.0
    else:
        raise ValueError(f"unsupported total line {line}")
    s=o+u+push
    return o/s,u/s,push/s

def _poisson_over_conditional(mu,line):
    frac=abs(float(line)-round(float(line)))
    if frac<1e-9:
        k=int(round(float(line)))
        u=float(poisson.cdf(k-1,mu)); o=float(1-poisson.cdf(k,mu))
    elif abs(frac-.5)<1e-9:
        k=int(math.floor(float(line)))
        u=float(poisson.cdf(k,mu)); o=float(1-u)
    else:
        raise ValueError(f"unsupported total line {line}")
    return o/(o+u)

def totals_nb_candidate_probabilities(model_home_runs,model_away_runs,total_line):
    obj=_load()
    o,u,push=_nb_probs(
        model_home_runs,
        model_away_runs,
        total_line,
        obj["production_alpha_home"],
        obj["production_alpha_away"],
    )
    nbq=float(_cal([o/(o+u)],obj["nb_calibrator"])[0])
    pq=_poisson_over_conditional(model_home_runs+model_away_runs,total_line)
    base=float(_beta([pq],obj["old_total_beta"])[0])
    w=float(obj["blend"])
    q=float(np.clip((1-w)*base+w*nbq,0,1))
    resolved=1.0-push
    return resolved*q,resolved*(1-q),push
'''


def patch_build(path):
    text=path.read_text(encoding="utf-8")
    if PATCH_IMPORT not in text:
        anchor="from scipy.stats import poisson, skellam"
        if anchor not in text:
            raise RuntimeError("Could not locate scipy.stats import")
        text=text.replace(anchor,anchor+"\n\n"+PATCH_IMPORT,1)

    sig="\n".join([
        "def totals_probabilities(",
        "    model_home_runs,",
        "    model_away_runs,",
        "    total_line,",
        "):",
    ])
    start=text.find(sig)
    if start<0:
        raise RuntimeError("Could not locate totals_probabilities signature")
    body_start=start+len(sig)
    lookahead=text[body_start:body_start+300]
    if "totals_nb_candidate_probabilities(" not in lookahead:
        text=text[:body_start]+"\n"+PATCH_RETURN+text[body_start:]

    path.write_text(text,encoding="utf-8")


def disable_old_total_calibrator(path):
    obj=json.loads(path.read_text(encoding="utf-8"))
    total=obj["markets"]["total"]
    total["enabled"]=False
    total["disabled_reason"]="Replaced by promoted negative-binomial totals model"
    path.write_text(json.dumps(obj,indent=2)+"\n",encoding="utf-8")


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--no-promote",action="store_true")
    args=ap.parse_args()

    root=repo_root()
    pred,dist=load_inputs(root)
    old_beta=current_total_beta(resolve(root,CAL_ARTIFACT))
    lines=sorted(pred["line"].unique().tolist())

    all_nb=build_nb_over_rows(dist,lines,["cv_fold_1","cv_fold_2","cv_fold_3","cv_fold_4"])
    eval_nb=all_nb[all_nb["period"].isin(["cv_fold_2","cv_fold_3","cv_fold_4"])].copy()
    eval_pred=pred[pred["period"].isin(["cv_fold_2","cv_fold_3","cv_fold_4"])].copy()
    base_rows=build_runtime_baseline_rows(
        dist,lines,["cv_fold_2","cv_fold_3","cv_fold_4","final_test"],old_beta
    )

    prelim=[]; cache={}; specmap={}
    for spec in specs():
        df=candidate_eval_rows(eval_pred,eval_nb,base_rows,all_nb,spec)
        row,_=evaluate(df,spec.name,False)
        prelim.append(row); cache[spec.name]=df; specmap[spec.name]=spec

    predf=pd.DataFrame(prelim).sort_values(["candidate_ll","candidate_ece"])
    promising=predf[
        predf["all_bucket_abs_errors_improved"]
        & predf["ll_not_worse"]
        & predf["ece_not_worse"]
    ].copy()

    gates=[]; bucket_frames=[]
    for name in promising["candidate"].tolist():
        row,buckets=evaluate(cache[name],name,True)
        gates.append(row); bucket_frames.append(buckets)

    passing=[r for r in gates if r["gate_passed"]]
    selected={"status":"no_candidate_passed","production_changed":False}
    selected_models=None; selected_spec=None

    if passing:
        best=sorted(passing,key=lambda r:(r["candidate_ll"],r["candidate_ece"]))[0]
        selected_spec=specmap[best["candidate"]]
        reference=all_nb["game_date"].max()+pd.Timedelta(days=1)
        selected_models=fit_side_models(all_nb,reference,selected_spec)

        artifact={
            "enabled":True,
            "market":"total",
            "distribution":"independent_negative_binomial",
            "production_alpha_home":PRODUCTION_ALPHA_HOME,
            "production_alpha_away":PRODUCTION_ALPHA_AWAY,
            "old_total_beta":old_beta,
            "nb_side_calibrators":selected_models,
            "blend":selected_spec.blend,
            "selected_candidate":best["candidate"],
            "fit_periods":["cv_fold_1","cv_fold_2","cv_fold_3","cv_fold_4"],
            "final_test_used_for_fit_or_selection":False,
        }

        selected={
            "status":"candidate_passed",
            "candidate":best,
            "production_changed":False,
            "artifact":artifact,
        }

        if not args.no_promote:
            build=resolve(root,BUILD_JUICE)
            cal=resolve(root,CAL_ARTIFACT)
            helper=resolve(root,NB_HELPER)
            art=resolve(root,NB_ARTIFACT)
            stamp=datetime.now().strftime("%Y%m%d_%H%M%S")
            build_backup=build.with_name(build.name+f".nbside_{stamp}.bak")
            cal_backup=cal.with_name(cal.name+f".nbside_{stamp}.bak")
            shutil.copy2(build,build_backup)
            shutil.copy2(cal,cal_backup)
            helper.parent.mkdir(parents=True,exist_ok=True)
            art.parent.mkdir(parents=True,exist_ok=True)
            helper.write_text(RUNTIME_HELPER,encoding="utf-8")
            art.write_text(json.dumps(artifact,indent=2)+"\n",encoding="utf-8")
            patch_build(build)
            disable_old_total_calibrator(cal)

            import py_compile
            py_compile.compile(str(helper),doraise=True)
            py_compile.compile(str(build),doraise=True)

            selected["production_changed"]=True
            selected["build_backup"]=str(build_backup)
            selected["calibration_backup"]=str(cal_backup)

    final_ref=[]
    final=pred[pred["period"].eq("final_test")].copy()
    if not final.empty:
        fb=base_rows[base_rows["period"].eq("final_test")].copy()
        final=final.merge(
            fb[["period","game_id","line","baseline_over_p"]],
            on=["period","game_id","line"],how="left",validate="many_to_one"
        )
        base=np.where(
            final["side"].eq("over"),
            final["baseline_over_p"],
            1.0-final["baseline_over_p"],
        )
        y=final["y"].to_numpy(float)
        final_ref.append({
            "system":"current_runtime_production",
            "rows":len(final),
            "log_loss":log_loss(y,base),
            "ece":ece(y,base),
        })

        if selected_models is not None:
            final_nb=build_nb_over_rows(dist,lines,["final_test"])
            final=final.merge(
                final_nb[["period","game_id","line","nb_over_p"]],
                on=["period","game_id","line"],how="left",validate="many_to_one"
            )
            q=apply_side_models(final["nb_over_p"].to_numpy(),selected_models)
            nbside=np.where(final["side"].eq("over"),q,1.0-q)
            cand=clip((1-selected_spec.blend)*base+selected_spec.blend*nbside)
            final_ref.append({
                "system":"selected_nb_side_reference",
                "rows":len(final),
                "log_loss":log_loss(y,cand),
                "ece":ece(y,cand),
            })

    outdir=resolve(root,OUTDIR)
    outdir.mkdir(parents=True,exist_ok=True)
    predf.to_csv(outdir/"candidate_preliminary_metrics.csv",index=False)
    pd.DataFrame(gates).to_csv(outdir/"candidate_gate_metrics.csv",index=False)
    if bucket_frames:
        pd.concat(bucket_frames,ignore_index=True).to_csv(outdir/"target_bucket_metrics.csv",index=False)
    pd.DataFrame(final_ref).to_csv(outdir/"final_test_reference_metrics.csv",index=False)
    (outdir/"selected_candidate.json").write_text(
        json.dumps(selected,indent=2)+"\n",encoding="utf-8"
    )

    print("NB SIDE-CALIBRATION RESULT")
    print(json.dumps(selected,indent=2))
    if gates:
        cols=[
            "candidate","baseline_ll","candidate_ll","baseline_ece","candidate_ece",
            "all_bucket_abs_errors_improved","all_confirmed_overstatements_removed",
            "ll_not_worse","ece_not_worse","gate_passed"
        ]
        print(pd.DataFrame(gates)[cols].to_string(index=False))
    else:
        print("No candidate reached bootstrap gate.")
    print("FINAL TEST REFERENCE ONLY")
    print(pd.DataFrame(final_ref).to_string(index=False) if final_ref else "No final rows")
    print("PRODUCTION CHANGED:",selected.get("production_changed",False))


if __name__=="__main__":
    main()

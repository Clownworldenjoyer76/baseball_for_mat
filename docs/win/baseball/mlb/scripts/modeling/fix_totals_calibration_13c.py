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
from scipy.special import expit
from scipy.stats import poisson
from sklearn.isotonic import IsotonicRegression

BASE = Path("docs/win/baseball/mlb")
PREDICTIONS = BASE / "modeling/probability_calibration/calibration_predictions.csv"
DIST_PROBS = BASE / "modeling/count_distribution_backtest/game_probabilities.csv"
CURRENT_ARTIFACT = BASE / "models/probability_calibration/market_calibrators.json"

OUTPUT_DIR = BASE / "modeling/totals_calibration_13c"
OVERLAY_ARTIFACT = BASE / "models/probability_calibration/totals_targeted_overlay.json"
OVERLAY_HELPER = BASE / "scripts/01_merge/totals_targeted_overlay.py"
BUILD_JUICE = BASE / "scripts/01_merge/build_juice_files.py"

TARGET_BUCKETS = [(0.30,0.40),(0.40,0.50),(0.50,0.60),(0.60,0.70),(0.70,0.80)]
ECE_EDGES = np.linspace(0.0,1.0,11)
MIN_P = 1e-8
TOL = 1e-12
BOOTSTRAP_DRAWS = 10000
SEED = 1313

PATCH_IMPORT = "from totals_targeted_overlay import apply_totals_targeted_overlay"
PATCH_CALL = "    tot = apply_totals_targeted_overlay(tot)"


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
    out=np.empty(BOOTSTRAP_DRAWS,float)
    pos=0
    while pos<BOOTSTRAP_DRAWS:
        k=min(500,BOOTSTRAP_DRAWS-pos)
        idx=rng.integers(0,n,size=(k,n))
        out[pos:pos+k]=d[idx].mean(axis=1)
        pos+=k
    lo,hi=np.quantile(out,[.025,.975])
    return float(lo),float(hi)


def fold_num(s):
    m=re.fullmatch(r"cv_fold_(\d+)",str(s))
    if not m:
        raise ValueError(f"bad fold {s}")
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
        raise RuntimeError(f"Missing columns: {sorted(miss)}")
    df=df[df["market"].eq("total")].copy()
    df["line"]=df["contract"].map(parse_line)
    df["game_date"]=pd.to_datetime(df["game_date"],errors="raise")
    for c in ("y","raw_p","calibrated_p"):
        df[c]=pd.to_numeric(df[c],errors="coerce")
    if df[["y","raw_p","calibrated_p"]].isna().any().any():
        raise RuntimeError("NaN in total calibration predictions")
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


def current_beta(path):
    obj=json.loads(path.read_text(encoding="utf-8"))
    total=obj.get("markets",{}).get("total")
    if total is None:
        raise RuntimeError("No markets.total in calibration artifact")
    for k in ("a","b","intercept"):
        if k not in total:
            raise RuntimeError(f"markets.total missing {k}")
    return float(total["a"]),float(total["b"]),float(total["intercept"])


def beta_apply(p,params):
    a,b,c=params; p=clip(p)
    return expit(c+a*np.log(p)-b*np.log1p(-p))


def raw_total(mu,line):
    frac=abs(line-round(line))
    if frac<1e-9:
        k=int(round(line))
        under=float(poisson.cdf(k-1,mu))
        push=float(poisson.pmf(k,mu))
        over=float(1-poisson.cdf(k,mu))
    elif abs(frac-.5)<1e-9:
        k=int(math.floor(line))
        under=float(poisson.cdf(k,mu))
        push=0.0
        over=float(1-under)
    else:
        raise ValueError(f"Unsupported total line {line}")
    return over,under,push


def reconstruct_fold1(pred,dist,beta):
    games=dist[dist["period"].eq("cv_fold_1")].copy()
    contracts=sorted(pred["contract"].astype(str).unique())
    rows=[]
    for g in games.itertuples(index=False):
        mu=float(g.mean_home_runs)+float(g.mean_away_runs)
        actual=float(g.actual_home_runs)+float(g.actual_away_runs)
        for contract in contracts:
            line=parse_line(contract)
            side=contract_side(contract)
            po,pu,_=raw_total(mu,line)
            if abs(actual-line)<1e-9:
                continue
            resolved=po+pu
            raw_over=po/resolved
            raw=raw_over if side=="over" else 1-raw_over
            y=float(actual>line) if side=="over" else float(actual<line)
            cal=float(beta_apply([raw],beta)[0])
            rows.append({
                "period":"cv_fold_1","game_date":g.game_date,"game_id":g.game_id,
                "contract":contract,"line":line,"y":y,"raw_p":raw,"calibrated_p":cal
            })
    out=pd.DataFrame(rows)
    if out.empty:
        raise RuntimeError("No fold1 reconstruction rows")
    return out


@dataclass(frozen=True)
class Spec:
    mode:str
    width:float
    shrink:float
    local_weight:float

    @property
    def name(self):
        return f"{self.mode}_w{self.width:g}_s{self.shrink:g}_lw{self.local_weight:g}"


def specs():
    out=[]
    for mode in ("global","line_blend"):
        for width in (0.05,0.10):
            for shrink in (0.15,0.25,0.35,0.5,0.65,0.8,1.0,1.2):
                lws=(0.0,) if mode=="global" else (0.25,0.5,0.75,1.0)
                for lw in lws:
                    out.append(Spec(mode,width,shrink,lw))
    return out


def residual_table(p,y,width):
    p=np.asarray(p,float); y=np.asarray(y,float)
    edges=np.arange(0.0,1.0+width/2,width)
    if edges[-1]<1:
        edges=np.append(edges,1.0)
    ids=np.digitize(np.clip(p,0,1),edges[1:-1],right=False)
    xs=[]; gaps=[]; counts=[]
    for i in range(len(edges)-1):
        m=ids==i
        if not m.any():
            continue
        xs.append(float(p[m].mean()))
        gaps.append(float((p[m]-y[m]).mean()))
        counts.append(int(m.sum()))
    return np.asarray(xs),np.asarray(gaps),np.asarray(counts)


def make_monotone_map(xs,ys,weights):
    anchors_x=np.concatenate(([0.0],np.asarray(xs,float),[1.0]))
    anchors_y=np.concatenate(([0.0],np.asarray(ys,float),[1.0]))
    weights=np.concatenate(([1.0],np.asarray(weights,float),[1.0]))
    order=np.argsort(anchors_x)
    anchors_x=anchors_x[order]; anchors_y=anchors_y[order]; weights=weights[order]
    iso=IsotonicRegression(increasing=True,out_of_bounds="clip")
    z=iso.fit_transform(anchors_x,anchors_y,sample_weight=weights)
    ux=[]; uy=[]
    for x in np.unique(anchors_x):
        m=anchors_x==x
        ux.append(float(x))
        uy.append(float(np.average(z[m],weights=weights[m])))
    return {"x":ux,"y":uy}


def fit_targeted(train,spec):
    p=train["calibrated_p"].to_numpy(float)
    y=train["y"].to_numpy(float)
    gx,gg,gc=residual_table(p,y,spec.width)

    def global_gap_at(x):
        if len(gx)==0:
            return np.zeros_like(np.asarray(x,float))
        return np.interp(x,gx,gg,left=gg[0],right=gg[-1])

    # Build a dense set of anchors from actual bin centers.
    anchors=sorted(set([0.0,0.3,0.4,0.5,0.6,0.7,0.8,1.0]+gx.tolist()))
    ay=[]; aw=[]
    for x in anchors:
        if x<=0.0 or x>=1.0:
            ay.append(x); aw.append(1.0); continue

        gap=float(global_gap_at([x])[0])

        if spec.mode=="line_blend":
            # Runtime map may vary by line; handled separately below.
            pass

        if 0.30 <= x <= 0.80:
            target=x-spec.shrink*gap
        else:
            target=x

        ay.append(float(np.clip(target,0,1)))
        # Stronger identity anchors outside target range.
        aw.append(10000.0 if not (0.30<=x<=0.80) else 100.0)

    global_map=make_monotone_map(anchors,ay,aw)
    model={"mode":spec.mode,"global":global_map,"by_line":{}}

    if spec.mode=="line_blend":
        for line,grp in train.groupby("line"):
            if len(grp)<25:
                continue
            lx,lg,lc=residual_table(grp["calibrated_p"].to_numpy(float),grp["y"].to_numpy(float),spec.width)
            line_anchors=sorted(set([0.0,0.3,0.4,0.5,0.6,0.7,0.8,1.0]+gx.tolist()+lx.tolist()))
            ly=[]; lw=[]
            for x in line_anchors:
                if x<=0 or x>=1:
                    ly.append(x); lw.append(1.0); continue
                ggap=float(global_gap_at([x])[0])
                if len(lx):
                    lgap=float(np.interp(x,lx,lg,left=lg[0],right=lg[-1]))
                else:
                    lgap=ggap
                gap=(1-spec.local_weight)*ggap+spec.local_weight*lgap
                target=x-spec.shrink*gap if 0.30<=x<=0.80 else x
                ly.append(float(np.clip(target,0,1)))
                lw.append(10000.0 if not (0.30<=x<=0.80) else 100.0)
            model["by_line"][str(float(line))]=make_monotone_map(line_anchors,ly,lw)

    return model


def interp_map(p,m):
    p=np.asarray(p,float)
    x=np.asarray(m["x"],float); y=np.asarray(m["y"],float)
    return clip(np.interp(p,x,y,left=y[0],right=y[-1]))


def apply_model(p,lines,model):
    p=np.asarray(p,float); lines=np.asarray(lines,float)
    out=interp_map(p,model["global"])
    if model["mode"]!="line_blend" or not model["by_line"]:
        return out

    known=np.asarray([float(k) for k in model["by_line"].keys()],float)
    for i,line in enumerate(lines):
        key=str(float(line))
        if key in model["by_line"]:
            out[i]=interp_map([p[i]],model["by_line"][key])[0]
        else:
            # Nearest trained line map for unseen but nearby runtime lines.
            nearest=float(known[np.argmin(np.abs(known-line))])
            out[i]=interp_map([p[i]],model["by_line"][str(nearest)])[0]
    return clip(out)


def chronological_predictions(all_cv,eval_rows,spec):
    outs=[]
    for period in ("cv_fold_2","cv_fold_3","cv_fold_4"):
        f=fold_num(period)
        tr=all_cv[all_cv["period"].map(fold_num)<f]
        va=eval_rows[eval_rows["period"].eq(period)].copy()
        model=fit_targeted(tr,spec)
        va["candidate_p"]=apply_model(
            va["calibrated_p"].to_numpy(),
            va["line"].to_numpy(),
            model,
        )
        outs.append(va)
    return pd.concat(outs,ignore_index=True)


def target_metrics(df,name,bootstrap):
    rows=[]; bp=df["calibrated_p"].to_numpy(float)
    for i,(lo,hi) in enumerate(TARGET_BUCKETS):
        m=(bp>=lo)&(bp<hi); s=df.loc[m]
        if len(s)<30:
            raise RuntimeError(f"Target bucket {lo}-{hi} only has {len(s)} rows")
        y=s["y"].to_numpy(float); b=s["calibrated_p"].to_numpy(float); c=s["candidate_p"].to_numpy(float)
        bg=float(b.mean()-y.mean()); cg=float(c.mean()-y.mean())
        clo=chi=float("nan")
        if bootstrap:
            clo,chi=bootstrap_ci(c,y,SEED+i+sum(map(ord,name)))
        rows.append({
            "candidate":name,"bucket":f"{lo:.1f}-{hi:.1f}","rows":len(s),
            "baseline_mean_pred":float(b.mean()),"candidate_mean_pred":float(c.mean()),"observed_rate":float(y.mean()),
            "baseline_gap":bg,"candidate_gap":cg,
            "baseline_abs_error":abs(bg),"candidate_abs_error":abs(cg),
            "abs_error_improved":bool(abs(cg)<abs(bg)-TOL),
            "candidate_ci95_low":clo,"candidate_ci95_high":chi,
            "confirmed_overstatement_removed":bool(clo<=TOL) if bootstrap else False,
        })
    return pd.DataFrame(rows)


def eval_candidate(df,name,bootstrap=False):
    y=df["y"].to_numpy(float); b=df["calibrated_p"].to_numpy(float); c=df["candidate_p"].to_numpy(float)
    buckets=target_metrics(df,name,bootstrap)
    row={
        "candidate":name,"rows":len(df),
        "baseline_ll":log_loss(y,b),"candidate_ll":log_loss(y,c),
        "baseline_ece":ece(y,b),"candidate_ece":ece(y,c),
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


RUNTIME_HELPER = '''#!/usr/bin/env python3
from __future__ import annotations
import json
from functools import lru_cache
from pathlib import Path
import numpy as np
import pandas as pd

ROOT=Path(__file__).resolve().parents[6]
ARTIFACT=ROOT/"docs/win/baseball/mlb/models/probability_calibration/totals_targeted_overlay.json"

@lru_cache(maxsize=1)
def _load():
    if not ARTIFACT.exists():
        return None
    obj=json.loads(ARTIFACT.read_text(encoding="utf-8"))
    if not obj.get("enabled",False):
        return None
    return obj

def _interp(p,m):
    x=np.asarray(m["x"],float); y=np.asarray(m["y"],float)
    return np.clip(np.interp(np.asarray(p,float),x,y,left=y[0],right=y[-1]),0,1)

def _apply(p,lines,obj):
    p=np.asarray(p,float); lines=np.asarray(lines,float)
    out=_interp(p,obj["model"]["global"])
    by_line=obj["model"].get("by_line",{})
    if obj["model"].get("mode")!="line_blend" or not by_line:
        return out
    known=np.asarray([float(k) for k in by_line],float)
    for i,line in enumerate(lines):
        key=str(float(line))
        if key in by_line:
            m=by_line[key]
        else:
            nearest=float(known[np.argmin(np.abs(known-line))])
            m=by_line[str(nearest)]
        out[i]=_interp([p[i]],m)[0]
    return np.clip(out,0,1)

def apply_totals_targeted_overlay(tot: pd.DataFrame) -> pd.DataFrame:
    obj=_load()
    if obj is None or tot.empty:
        return tot
    req=["total","over_model_prob_total_win","under_model_prob_total_win","total_model_prob_push"]
    missing=[c for c in req if c not in tot.columns]
    if missing:
        raise ValueError(f"targeted totals overlay missing columns: {missing}")
    out=tot.copy()
    lines=pd.to_numeric(out["total"],errors="raise").to_numpy(float)
    ow=pd.to_numeric(out["over_model_prob_total_win"],errors="raise").to_numpy(float)
    uw=pd.to_numeric(out["under_model_prob_total_win"],errors="raise").to_numpy(float)
    push=pd.to_numeric(out["total_model_prob_push"],errors="raise").to_numpy(float)
    resolved=ow+uw
    if np.any(~np.isfinite(resolved)) or np.any(resolved<=0):
        raise ValueError("invalid resolved totals mass")
    q=_apply(ow/resolved,lines,obj)
    ow2=resolved*q
    uw2=resolved*(1-q)
    out["over_model_prob_total_win"]=ow2
    out["over_model_prob_total_loss"]=uw2
    out["under_model_prob_total_win"]=uw2
    out["under_model_prob_total_loss"]=ow2
    out["total_model_prob_push"]=push
    return out
'''


def patch_build(path):
    text=path.read_text(encoding="utf-8")
    if PATCH_IMPORT not in text:
        anchor="from scipy.stats import poisson, skellam"
        if anchor not in text:
            raise RuntimeError("Could not locate scipy.stats import")
        text=text.replace(anchor,anchor+"\n\n"+PATCH_IMPORT,1)
    if PATCH_CALL not in text:
        pat=re.compile(r'(?P<block>(?P<indent>[ \t]*)tot\s*\[\s*["\']total_model_prob_push["\']\s*\]\s*=\s*pushes\s*)',re.MULTILINE)
        m=pat.search(text)
        if not m:
            raise RuntimeError("Could not locate total_model_prob_push assignment")
        end=m.end("block")
        text=text[:end]+"\n\n"+PATCH_CALL+"\n"+text[end:]
    path.write_text(text,encoding="utf-8")


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--no-promote",action="store_true")
    args=ap.parse_args()

    root=repo_root()
    pred=load_predictions(resolve(root,PREDICTIONS))
    dist=load_dist(resolve(root,DIST_PROBS))
    beta=current_beta(resolve(root,CURRENT_ARTIFACT))

    fold1=reconstruct_fold1(pred,dist,beta)
    cv=pred[pred["period"].isin(["cv_fold_2","cv_fold_3","cv_fold_4"])].copy()
    all_cv=pd.concat([fold1,cv],ignore_index=True)
    eval_rows=cv.copy()

    prelim=[]; cache={}; specmap={}
    for spec in specs():
        df=chronological_predictions(all_cv,eval_rows,spec)
        row,_=eval_candidate(df,spec.name,False)
        prelim.append(row); cache[spec.name]=df; specmap[spec.name]=spec

    predf=pd.DataFrame(prelim)
    promising=predf[
        predf["all_bucket_abs_errors_improved"] &
        predf["ll_not_worse"] &
        predf["ece_not_worse"]
    ].sort_values(["candidate_ll","candidate_ece"])

    gates=[]; bucket_frames=[]
    for name in promising["candidate"].tolist():
        row,buck=eval_candidate(cache[name],name,True)
        gates.append(row); bucket_frames.append(buck)

    passing=[r for r in gates if r["gate_passed"]]
    selected={"status":"no_candidate_passed","production_changed":False}
    final_model=None

    if passing:
        best=sorted(passing,key=lambda r:(r["candidate_ll"],r["candidate_ece"]))[0]
        spec=specmap[best["candidate"]]
        final_model=fit_targeted(all_cv,spec)

        artifact_obj={
            "enabled":True,
            "market":"total",
            "stage":"post_current_total_beta_calibration",
            "selected_candidate":best["candidate"],
            "model":final_model,
            "fit_periods":["cv_fold_1","cv_fold_2","cv_fold_3","cv_fold_4"],
            "fit_rows":int(len(all_cv)),
            "final_test_used_for_fit_or_selection":False,
        }

        outdir=resolve(root,OUTPUT_DIR); outdir.mkdir(parents=True,exist_ok=True)
        (outdir/"totals_targeted_overlay_candidate.json").write_text(json.dumps(artifact_obj,indent=2)+"\n",encoding="utf-8")
        (outdir/"totals_targeted_overlay_candidate.py").write_text(RUNTIME_HELPER,encoding="utf-8")

        selected={
            "status":"candidate_passed",
            "candidate":best,
            "production_changed":False,
            "model":artifact_obj,
        }

        if not args.no_promote:
            build=resolve(root,BUILD_JUICE); helper=resolve(root,OVERLAY_HELPER); art=resolve(root,OVERLAY_ARTIFACT)
            stamp=datetime.now().strftime("%Y%m%d_%H%M%S")
            backup=build.with_name(build.name+f".item13c_{stamp}.bak")
            shutil.copy2(build,backup)
            helper.parent.mkdir(parents=True,exist_ok=True); art.parent.mkdir(parents=True,exist_ok=True)
            helper.write_text(RUNTIME_HELPER,encoding="utf-8")
            art.write_text(json.dumps(artifact_obj,indent=2)+"\n",encoding="utf-8")
            patch_build(build)

            import py_compile
            py_compile.compile(str(helper),doraise=True)
            py_compile.compile(str(build),doraise=True)

            selected["production_changed"]=True
            selected["runtime_helper"]=str(helper)
            selected["artifact"]=str(art)
            selected["build_juice_backup"]=str(backup)

    final_ref=[]
    final=pred[pred["period"].eq("final_test")].copy()
    if not final.empty:
        y=final["y"].to_numpy(float); b=final["calibrated_p"].to_numpy(float)
        final_ref.append({"system":"current_production_before_13c","rows":len(final),"log_loss":log_loss(y,b),"ece":ece(y,b)})
        if final_model is not None:
            q=apply_model(b,final["line"].to_numpy(),final_model)
            final_ref.append({"system":"selected_13c_reference","rows":len(final),"log_loss":log_loss(y,q),"ece":ece(y,q)})

    outdir=resolve(root,OUTPUT_DIR); outdir.mkdir(parents=True,exist_ok=True)
    predf.to_csv(outdir/"candidate_preliminary_metrics.csv",index=False)
    pd.DataFrame(gates).to_csv(outdir/"candidate_gate_metrics.csv",index=False)
    if bucket_frames:
        pd.concat(bucket_frames,ignore_index=True).to_csv(outdir/"target_bucket_metrics.csv",index=False)
    pd.DataFrame(final_ref).to_csv(outdir/"final_test_reference_metrics.csv",index=False)
    (outdir/"selected_candidate.json").write_text(json.dumps(selected,indent=2)+"\n",encoding="utf-8")

    print("ITEM 13C RESULT")
    print(json.dumps(selected,indent=2))
    if gates:
        cols=["candidate","baseline_ll","candidate_ll","baseline_ece","candidate_ece","all_bucket_abs_errors_improved","all_confirmed_overstatements_removed","gate_passed"]
        print(pd.DataFrame(gates)[cols].to_string(index=False))
    print("FINAL TEST REFERENCE ONLY")
    print(pd.DataFrame(final_ref).to_string(index=False) if final_ref else "No final_test rows")
    print("PRODUCTION CHANGED:",selected.get("production_changed",False))


if __name__=="__main__":
    main()

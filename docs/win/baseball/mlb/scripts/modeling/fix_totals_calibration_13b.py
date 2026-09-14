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
OUTPUT_DIR = BASE / "modeling/totals_calibration_13b"
OVERLAY_ARTIFACT = BASE / "models/probability_calibration/totals_overlay.json"
OVERLAY_HELPER = BASE / "scripts/01_merge/totals_calibration_overlay.py"
BUILD_JUICE = BASE / "scripts/01_merge/build_juice_files.py"

TARGET_BUCKETS = [(0.30,0.40),(0.40,0.50),(0.50,0.60),(0.60,0.70),(0.70,0.80)]
ECE_EDGES = np.linspace(0,1,11)
MIN_P=1e-8
MIN_BUCKET_ROWS=30
BOOTSTRAP_DRAWS=10000
SEED=1313
TOL=1e-12

PATCH_IMPORT="from totals_calibration_overlay import apply_totals_overlay_df"
PATCH_CALL="    tot = apply_totals_overlay_df(tot)"

def repo_root():
    for start in [Path.cwd().resolve(), Path(__file__).resolve().parent]:
        for p in (start,*start.parents):
            if (p/BASE).is_dir():
                return p
    raise RuntimeError("Could not locate repo root")

def resolve(root,p): return p if p.is_absolute() else root/p
def clip(p): return np.clip(np.asarray(p,float),MIN_P,1-MIN_P)

def ll(y,p):
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

def bootstrap_ci_gap(p,y,seed):
    p=np.asarray(p,float); y=np.asarray(y,float); d=p-y; n=len(d)
    rng=np.random.default_rng(seed); means=np.empty(BOOTSTRAP_DRAWS,float)
    pos=0
    while pos<BOOTSTRAP_DRAWS:
        k=min(500,BOOTSTRAP_DRAWS-pos)
        idx=rng.integers(0,n,size=(k,n))
        means[pos:pos+k]=d[idx].mean(axis=1); pos+=k
    lo,hi=np.quantile(means,[.025,.975])
    return float(lo),float(hi)

def fold_num(s):
    m=re.fullmatch(r"cv_fold_(\d+)",str(s))
    if not m: raise ValueError(f"Bad fold label: {s}")
    return int(m.group(1))

def parse_total_line(contract):
    vals=re.findall(r"(?<!\d)(\d+(?:\.\d+)?)(?!\d)",str(contract))
    if not vals: raise ValueError(f"Cannot parse total line from {contract!r}")
    return float(vals[-1])

def contract_side(contract):
    return "under" if "under" in str(contract).lower() else "over"

def load_predictions(path):
    df=pd.read_csv(path)
    need={"market","period","game_date","game_id","contract","y","raw_p","calibrated_p"}
    miss=need-set(df.columns)
    if miss: raise RuntimeError(f"Missing prediction columns: {sorted(miss)}")
    df=df[df["market"].eq("total")].copy()
    df["game_date"]=pd.to_datetime(df["game_date"],errors="raise")
    for c in ["y","raw_p","calibrated_p"]:
        df[c]=pd.to_numeric(df[c],errors="coerce")
    if df[["y","raw_p","calibrated_p"]].isna().any().any():
        raise RuntimeError("NaN in total calibration predictions")
    return df.reset_index(drop=True)

def load_poisson(path):
    df=pd.read_csv(path)
    need={"period","distribution","game_date","game_id","actual_home_runs","actual_away_runs","mean_home_runs","mean_away_runs"}
    miss=need-set(df.columns)
    if miss: raise RuntimeError(f"Missing item #6 columns: {sorted(miss)}")
    df=df[df["distribution"].eq("poisson_skellam")].copy()
    df["game_date"]=pd.to_datetime(df["game_date"],errors="raise")
    return df.reset_index(drop=True)

def find_abc(node,path="root"):
    found=[]
    if isinstance(node,dict):
        if {"a","b","c"}.issubset(node) and all(isinstance(node[k],(int,float)) for k in ("a","b","c")):
            found.append((float(node["a"]),float(node["b"]),float(node["c"]),path))
        for k,v in node.items(): found.extend(find_abc(v,f"{path}.{k}"))
    elif isinstance(node,list):
        for i,v in enumerate(node): found.extend(find_abc(v,f"{path}[{i}]"))
    return found

def current_total_params(path):
    obj=json.loads(path.read_text(encoding="utf-8"))
    total=obj.get("markets",{}).get("total")
    if total is None:
        raise RuntimeError("Current artifact has no markets.total")
    if not all(k in total for k in ("a","b","intercept")):
        raise RuntimeError("markets.total must contain a, b, and intercept")
    return float(total["a"]), float(total["b"]), float(total["intercept"])

def beta_apply(p,params):
    a,b,c=params; p=clip(p)
    return expit(c+a*np.log(p)-b*np.log1p(-p))

def reconstruct_fold1(item8,item6,beta_params):
    games=item6[item6["period"].eq("cv_fold_1")].copy()
    contracts=sorted(item8["contract"].astype(str).unique())
    rows=[]
    for g in games.itertuples(index=False):
        mu=float(g.mean_home_runs)+float(g.mean_away_runs)
        actual=float(g.actual_home_runs)+float(g.actual_away_runs)
        for contract in contracts:
            line=parse_total_line(contract); side=contract_side(contract)
            if abs(line-round(line))<1e-9:
                k=int(round(line)); pu=float(poisson.cdf(k-1,mu)); po=float(1-poisson.cdf(k,mu))
            else:
                k=int(math.floor(line)); pu=float(poisson.cdf(k,mu)); po=float(1-pu)
            if abs(actual-line)<1e-9: continue
            resolved=po+pu
            raw_over=po/resolved
            raw=raw_over if side=="over" else 1-raw_over
            y=float(actual>line) if side=="over" else float(actual<line)
            cal=float(beta_apply([raw],beta_params)[0])
            rows.append({"market":"total","period":"cv_fold_1","game_date":g.game_date,"game_id":g.game_id,"contract":contract,"y":y,"raw_p":raw,"calibrated_p":cal})
    out=pd.DataFrame(rows)
    if out.empty: raise RuntimeError("No cv_fold_1 reconstruction rows")
    return out

@dataclass(frozen=True)
class Spec:
    family:str
    bins:int=0
    pseudo:float=0.0
    blend:float=1.0
    @property
    def name(self):
        if self.family=="isotonic":
            return f"isotonic_blend_{self.blend:g}"
        return f"{self.family}_bins{self.bins}_pseudo{self.pseudo:g}_blend{self.blend:g}"

def pav_xy(x,y,w):
    iso=IsotonicRegression(increasing=True,out_of_bounds="clip")
    yy=iso.fit_transform(np.asarray(x,float),np.asarray(y,float),sample_weight=np.asarray(w,float))
    xx=np.asarray(x,float); ww=np.asarray(w,float)
    ux=[]; uy=[]
    for v in np.unique(xx):
        m=xx==v; ux.append(float(v)); uy.append(float(np.average(yy[m],weights=ww[m])))
    return np.asarray(ux),np.asarray(uy)

def fit_mapping(p,y,spec):
    p=np.asarray(p,float); y=np.asarray(y,float)
    if spec.family=="isotonic":
        iso=IsotonicRegression(increasing=True,out_of_bounds="clip").fit(p,y)
        return {"x":iso.X_thresholds_.astype(float).tolist(),"y":iso.y_thresholds_.astype(float).tolist(),"blend":float(spec.blend),"family":spec.family}

    if spec.family=="quantile_piecewise":
        edges=np.unique(np.quantile(p,np.linspace(0,1,spec.bins+1)))
    elif spec.family=="fixed_piecewise":
        edges=np.linspace(0,1,spec.bins+1)
    else:
        raise ValueError(spec.family)

    if len(edges)<3: raise RuntimeError("Insufficient bin edges")
    ids=np.digitize(np.clip(p,0,1),edges[1:-1],right=False)
    xs=[]; ys=[]; ws=[]
    for i in range(len(edges)-1):
        m=ids==i
        if not m.any(): continue
        n=int(m.sum()); mp=float(p[m].mean()); rate=float(y[m].mean())
        target=(n*rate+spec.pseudo*mp)/(n+spec.pseudo)
        xs.append(mp); ys.append(target); ws.append(n+spec.pseudo)
    xs=[0.0]+xs+[1.0]; ys=[0.0]+ys+[1.0]; ws=[1.0]+ws+[1.0]
    x,z=pav_xy(xs,ys,ws)
    return {"x":x.tolist(),"y":z.tolist(),"blend":float(spec.blend),"family":spec.family}

def apply_mapping(p,model):
    p=np.asarray(p,float)
    x=np.asarray(model["x"],float); y=np.asarray(model["y"],float)
    mapped=np.interp(p,x,y,left=y[0],right=y[-1])
    a=float(model.get("blend",1.0))
    return np.clip((1-a)*p+a*mapped,MIN_P,1-MIN_P)

def specs():
    out=[Spec("isotonic",blend=a) for a in (.25,.5,.75,1.0)]
    for bins in (6,8,10,12,15,20,25,30):
        for pseudo in (0.0,10.0,25.0,50.0,100.0):
            for blend in (.5,.75,1.0):
                out.append(Spec("quantile_piecewise",bins,pseudo,blend))
    for bins in (10,20):
        for pseudo in (0.0,10.0,25.0,50.0,100.0):
            for blend in (.5,.75,1.0):
                out.append(Spec("fixed_piecewise",bins,pseudo,blend))
    return out

def chronological_predictions(all_cv,eval_rows,spec):
    outs=[]
    for period in ("cv_fold_2","cv_fold_3","cv_fold_4"):
        f=fold_num(period)
        tr=all_cv[all_cv["period"].map(fold_num)<f]
        va=eval_rows[eval_rows["period"].eq(period)].copy()
        model=fit_mapping(tr["calibrated_p"].to_numpy(),tr["y"].to_numpy(),spec)
        va["candidate_p"]=apply_mapping(va["calibrated_p"].to_numpy(),model)
        outs.append(va)
    return pd.concat(outs,ignore_index=True)

def target_metrics(df,name,bootstrap):
    rows=[]; bp=df["calibrated_p"].to_numpy(float)
    for i,(lo,hi) in enumerate(TARGET_BUCKETS):
        m=(bp>=lo)&(bp<hi); s=df.loc[m]
        if len(s)<MIN_BUCKET_ROWS: raise RuntimeError(f"Target bucket {lo}-{hi} has only {len(s)} rows")
        y=s["y"].to_numpy(float); b=s["calibrated_p"].to_numpy(float); c=s["candidate_p"].to_numpy(float)
        bg=float(b.mean()-y.mean()); cg=float(c.mean()-y.mean())
        clo=chi=float("nan")
        if bootstrap: clo,chi=bootstrap_ci_gap(c,y,SEED+i+sum(map(ord,name)))
        rows.append({
            "candidate":name,"bucket":f"{lo:.1f}-{hi:.1f}","rows":len(s),
            "baseline_mean_pred":float(b.mean()),"candidate_mean_pred":float(c.mean()),"observed_rate":float(y.mean()),
            "baseline_gap":bg,"candidate_gap":cg,"baseline_abs_error":abs(bg),"candidate_abs_error":abs(cg),
            "abs_error_improved":abs(cg)<abs(bg)-TOL,
            "candidate_ci95_low":clo,"candidate_ci95_high":chi,
            "confirmed_overstatement_removed":bool(clo<=TOL) if bootstrap else False
        })
    return pd.DataFrame(rows)

def preliminary_eval(df,name):
    y=df["y"].to_numpy(float); b=df["calibrated_p"].to_numpy(float); c=df["candidate_p"].to_numpy(float)
    buck=target_metrics(df,name,False)
    return {
        "candidate":name,"rows":len(df),
        "baseline_ll":ll(y,b),"candidate_ll":ll(y,c),
        "baseline_ece":ece(y,b),"candidate_ece":ece(y,c),
        "all_bucket_abs_errors_improved":bool(buck["abs_error_improved"].all()),
        "ll_not_worse":ll(y,c)<=ll(y,b)+TOL,
        "ece_not_worse":ece(y,c)<=ece(y,b)+TOL,
    }

def gate_eval(df,name):
    row=preliminary_eval(df,name)
    buck=target_metrics(df,name,True)
    row["all_confirmed_overstatements_removed"]=bool(buck["confirmed_overstatement_removed"].all())
    row["gate_passed"]=bool(row["all_bucket_abs_errors_improved"] and row["all_confirmed_overstatements_removed"] and row["ll_not_worse"] and row["ece_not_worse"])
    return row,buck

RUNTIME_HELPER = '''#!/usr/bin/env python3
from __future__ import annotations
import json
from functools import lru_cache
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[6]
ARTIFACT = ROOT / "docs/win/baseball/mlb/models/probability_calibration/totals_overlay.json"

@lru_cache(maxsize=1)
def _load():
    if not ARTIFACT.exists():
        return None
    obj=json.loads(ARTIFACT.read_text(encoding="utf-8"))
    if not bool(obj.get("enabled",False)):
        return None
    x=np.asarray(obj["x"],float); y=np.asarray(obj["y"],float); blend=float(obj.get("blend",1.0))
    if len(x)<2 or len(x)!=len(y):
        raise ValueError("invalid totals overlay artifact")
    if np.any(np.diff(x)<0) or np.any(np.diff(y)<-1e-12):
        raise ValueError("totals overlay must be monotone")
    return x,y,blend

def _map_probability(p):
    loaded=_load(); p=np.asarray(p,float)
    if loaded is None: return p
    x,y,blend=loaded
    mapped=np.interp(p,x,y,left=y[0],right=y[-1])
    return np.clip((1-blend)*p+blend*mapped,0,1)

def apply_totals_overlay_df(tot: pd.DataFrame) -> pd.DataFrame:
    if _load() is None or tot.empty:
        return tot
    required=["over_model_prob_total_win","over_model_prob_total_loss","under_model_prob_total_win","under_model_prob_total_loss","total_model_prob_push"]
    missing=[c for c in required if c not in tot.columns]
    if missing: raise ValueError(f"totals overlay missing columns: {missing}")
    out=tot.copy()
    push=pd.to_numeric(out["total_model_prob_push"],errors="raise").to_numpy(float)
    ow=pd.to_numeric(out["over_model_prob_total_win"],errors="raise").to_numpy(float)
    uw=pd.to_numeric(out["under_model_prob_total_win"],errors="raise").to_numpy(float)
    resolved=ow+uw
    if np.any(~np.isfinite(resolved)) or np.any(resolved<=0):
        raise ValueError("invalid resolved total mass before overlay")
    q2=_map_probability(ow/resolved)
    ow2=resolved*q2; uw2=resolved*(1-q2)
    out["over_model_prob_total_win"]=ow2
    out["over_model_prob_total_loss"]=uw2
    out["under_model_prob_total_win"]=uw2
    out["under_model_prob_total_loss"]=ow2
    out["total_model_prob_push"]=push
    return out
'''

def patch_build_juice(path):
    text=path.read_text(encoding="utf-8")
    if PATCH_IMPORT not in text:
        anchor="from scipy.stats import poisson, skellam"
        if anchor not in text: raise RuntimeError("Could not locate scipy.stats import")
        text=text.replace(anchor,anchor+"\\n\\n"+PATCH_IMPORT,1)
    if PATCH_CALL not in text:
        pat=re.compile(r'(?P<block>(?P<indent>[ \\t]*)tot\\s*\\[\\s*["\\\']total_model_prob_push["\\\']\\s*\\]\\s*=\\s*pushes\\s*)',re.MULTILINE)
        m=pat.search(text)
        if not m: raise RuntimeError("Could not locate total_model_prob_push assignment")
        end=m.end("block")
        text=text[:end]+"\\n\\n"+PATCH_CALL+"\\n"+text[end:]
    path.write_text(text,encoding="utf-8")

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--no-promote",action="store_true")
    args=ap.parse_args()

    root=repo_root()
    pred=load_predictions(resolve(root,PREDICTIONS))
    dist=load_poisson(resolve(root,DIST_PROBS))
    beta=current_total_params(resolve(root,CURRENT_ARTIFACT))

    fold1=reconstruct_fold1(pred,dist,beta)
    cv_existing=pred[pred["period"].isin(["cv_fold_2","cv_fold_3","cv_fold_4"])].copy()
    all_cv=pd.concat([fold1,cv_existing],ignore_index=True)
    eval_rows=cv_existing.copy()

    bp=eval_rows["calibrated_p"].to_numpy(float)
    baseline=[]
    for lo,hi in TARGET_BUCKETS:
        m=(bp>=lo)&(bp<hi); s=eval_rows.loc[m]
        gap=float(s["calibrated_p"].mean()-s["y"].mean())
        baseline.append((lo,hi,len(s),gap))
    if not all(n>=MIN_BUCKET_ROWS and gap>0 for _,_,n,gap in baseline):
        raise RuntimeError(f"Target overstatement did not reproduce: {baseline}")

    prelim=[]; pred_cache={}; spec_map={}
    for spec in specs():
        df=chronological_predictions(all_cv,eval_rows,spec)
        prelim.append(preliminary_eval(df,spec.name))
        pred_cache[spec.name]=df; spec_map[spec.name]=spec

    predf=pd.DataFrame(prelim)
    promising=predf[
        predf["all_bucket_abs_errors_improved"] &
        predf["ll_not_worse"] &
        predf["ece_not_worse"]
    ].sort_values(["candidate_ll","candidate_ece"])

    gates=[]; buckets=[]
    for name in promising["candidate"].tolist():
        row,buck=gate_eval(pred_cache[name],name)
        gates.append(row); buckets.append(buck)

    passing=[r for r in gates if r["gate_passed"]]
    selected={"status":"no_candidate_passed","production_changed":False}
    selected_model=None

    if passing:
        best=sorted(passing,key=lambda r:(r["candidate_ll"],r["candidate_ece"]))[0]
        name=best["candidate"]; spec=spec_map[name]
        selected_model=fit_mapping(all_cv["calibrated_p"].to_numpy(float),all_cv["y"].to_numpy(float),spec)
        overlay={
            "enabled":True,"market":"total","stage":"post_current_total_calibration",
            "method":"monotone_piecewise_overlay","candidate":name,
            "x":selected_model["x"],"y":selected_model["y"],"blend":selected_model["blend"],
            "fit_rows":int(len(all_cv)),
            "fit_periods":["cv_fold_1","cv_fold_2","cv_fold_3","cv_fold_4"],
            "final_test_used_for_fit_or_selection":False
        }
        selected={"status":"candidate_passed","candidate":name,"production_changed":False,"chronological_oos_metrics":best,"model":selected_model}

        outdir=resolve(root,OUTPUT_DIR); outdir.mkdir(parents=True,exist_ok=True)
        (outdir/"totals_overlay_candidate.json").write_text(json.dumps(overlay,indent=2)+"\\n",encoding="utf-8")
        (outdir/"totals_calibration_overlay_candidate.py").write_text(RUNTIME_HELPER,encoding="utf-8")

        if not args.no_promote:
            build=resolve(root,BUILD_JUICE); helper=resolve(root,OVERLAY_HELPER); art=resolve(root,OVERLAY_ARTIFACT)
            stamp=datetime.now().strftime("%Y%m%d_%H%M%S")
            backup=build.with_name(build.name+f".item13b_{stamp}.bak")
            shutil.copy2(build,backup)
            art.parent.mkdir(parents=True,exist_ok=True); helper.parent.mkdir(parents=True,exist_ok=True)
            art.write_text(json.dumps(overlay,indent=2)+"\\n",encoding="utf-8")
            helper.write_text(RUNTIME_HELPER,encoding="utf-8")
            patch_build_juice(build)
            import py_compile
            py_compile.compile(str(helper),doraise=True)
            py_compile.compile(str(build),doraise=True)
            selected.update({"production_changed":True,"overlay_artifact":str(art),"runtime_helper":str(helper),"build_juice_backup":str(backup)})

    finalref=[]
    final=pred[pred["period"].eq("final_test")].copy()
    if not final.empty:
        y=final["y"].to_numpy(float); basep=final["calibrated_p"].to_numpy(float)
        finalref.append({"system":"current_production_before_13b","rows":len(final),"log_loss":ll(y,basep),"ece":ece(y,basep)})
        if selected_model is not None:
            q=apply_mapping(basep,selected_model)
            finalref.append({"system":"selected_13b_reference","rows":len(final),"log_loss":ll(y,q),"ece":ece(y,q)})

    outdir=resolve(root,OUTPUT_DIR); outdir.mkdir(parents=True,exist_ok=True)
    predf.to_csv(outdir/"candidate_preliminary_metrics.csv",index=False)
    pd.DataFrame(gates).to_csv(outdir/"candidate_gate_metrics.csv",index=False)
    if buckets: pd.concat(buckets,ignore_index=True).to_csv(outdir/"target_bucket_metrics.csv",index=False)
    pd.DataFrame(finalref).to_csv(outdir/"final_test_reference_metrics.csv",index=False)
    (outdir/"selected_candidate.json").write_text(json.dumps(selected,indent=2)+"\\n",encoding="utf-8")

    print("ITEM 13B RESULT")
    print(json.dumps(selected,indent=2))
    if gates:
        cols=["candidate","baseline_ll","candidate_ll","baseline_ece","candidate_ece","all_bucket_abs_errors_improved","all_confirmed_overstatements_removed","ll_not_worse","ece_not_worse","gate_passed"]
        print(pd.DataFrame(gates)[cols].to_string(index=False))
    print("FINAL TEST REFERENCE ONLY")
    print(pd.DataFrame(finalref).to_string(index=False) if finalref else "No final_test rows")
    print("PRODUCTION CHANGED:",selected.get("production_changed",False))

if __name__=="__main__":
    main()

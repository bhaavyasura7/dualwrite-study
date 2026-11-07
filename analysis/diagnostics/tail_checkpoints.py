# Do high-tail cycles coincide with Postgres checkpoints?
import pandas as pd,sys,subprocess,re
d=pd.read_csv(sys.argv[1])
logs=subprocess.run(["docker","logs","dualwrite-postgres-primary-1"],capture_output=True,text=True)
cps=[]
for l in (logs.stdout+logs.stderr).splitlines():
    m=re.match(r'(\S+ \S+) GMT .*checkpoint complete:.*write=([\d.]+) s, sync=([\d.]+) s, total=([\d.]+) s',l)
    if m:
        end=pd.Timestamp(m[1]+"+00:00"); cps.append((end-pd.Timedelta(seconds=float(m[4])),end,float(m[3])))
d["s"]=pd.to_datetime(d.started_at); d["e"]=pd.to_datetime(d.finished_at)
d["cp"]=d.apply(lambda r:any(a<=r.e and b>=r.s for a,b,_ in cps),axis=1)
print("checkpoints:",len(cps),"| sync s max:",max(x for *_,x in cps))
print(d.groupby("cp").latency_p99_ms.agg(["count","median","mean","max"]).round(1).to_string())
print(d.sort_values("latency_p99_ms").tail(6)[["latency_p99_ms","cp"]].to_string(index=False))

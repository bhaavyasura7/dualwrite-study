import json,sys,pandas as pd
for n in ("load1","load2","load3"):
    r=[]
    for l in open(f"{sys.argv[1]}/{n}.log"):
        try:e=json.loads(l)
        except ValueError: continue
        if isinstance(e,dict) and "time" in e: r.append(e)
    d=pd.DataFrame(r); d["t"]=pd.to_datetime(d.time)
    c=d[d.msg=="CRASH_POINT_HIT"].t.min(); pre=d[d.t<=c]
    acked=set(pre[pre.msg=="SEND_ACKED"].event_id); s=pre[pre.msg=="SENT_CALLED"]
    un=s[~s.event_id.isin(acked)]
    ages=((c-un.t).dt.total_seconds()*1000).round(1)
    print(n,"unacked at crash",len(un),"oldest ms",ages.max(),"sent in last 100ms",int((((c-s.t).dt.total_seconds())<.1).sum()),"total sent",len(s))

import gzip,glob,json,os,sys
import pandas as pd
d=sys.argv[1]; rows=[]
for f in sorted(glob.glob(d+"/*.fault_times"),key=os.path.getmtime):
    b=f[:-len(".fault_times")]; label=os.path.basename(b)
    t0,t1=[pd.Timestamp(float(x),unit="s",tz="UTC") for x in open(f).read().split()]
    fails,retries,exhausted,acks=[],0,0,0
    for l in gzip.open(b+".log.gz","rt"):
        try:e=json.loads(l)
        except ValueError: continue
        if not isinstance(e,dict): continue
        m=e.get("msg")
        if m=="SEND_FAILED": fails.append(pd.Timestamp(e["time"]))
        retries+=m=="RETRY_SCHEDULED"; exhausted+=m=="RETRY_EXHAUSTED"
    r=json.load(open(b+".json"))
    rows.append(dict(case=label,first_fail_s=round((min(fails)-t0).total_seconds(),1) if fails else None,restored_s=round((t1-t0).total_seconds(),1),
      failed_during=sum(x<t1 for x in fails),failed_total=len(fails),retries=retries,exhausted=exhausted,lost=r["lost"],dup=r["duplicates"],ooo=r["out_of_order"],stale=r["stale_redelivery"],window_max_s=round(r["window_max_ms"]/1000,2)))
print(pd.DataFrame(rows).to_string(index=False))

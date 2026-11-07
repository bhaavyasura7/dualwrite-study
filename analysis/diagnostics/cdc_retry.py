import gzip,json,sys,os
import pandas as pd
T=sys.argv[1]; d=pd.read_csv(f"{T}/x1.csv")
for _,r in d.iterrows():
    j=json.load(open(f"{T}/damage/{r.cycle_id}.json")); txt=gzip.open(f"{T}/logs/{r.cycle_id}.log.gz","rt").read()
    sl=j.get("slot_timeline") or []
    lsns=[s["confirmed_flush_lsn"] for s in sl if s["slot"].endswith(r.cycle_id.lower().replace("-","_"))]
    inactive=sum(1 for s in sl if not s["active"])
    print(r.cycle_id,r.status,"events",r.events_total,"lost",r.lost,"dup",r.duplicates,"stale",r.stale_redelivery,
          "| retry_wait",txt.count("Going to restart connector"),"conn_errors",txt.count("PSQLException"),
          "| slot samples",len(sl),"inactive",inactive,"lsn moves",len(set(lsns)))

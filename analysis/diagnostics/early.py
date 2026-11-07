import json,sys,glob,pandas as pd
for f in sorted(glob.glob(sys.argv[1]+"/load*.log")):
    r=[]
    for l in open(f):
        try:e=json.loads(l)
        except ValueError: continue
        if isinstance(e,dict) and "time" in e: r.append(e)
    d=pd.DataFrame(r); d["t"]=pd.to_datetime(d.time)
    s=d[d.msg=="SENT_CALLED"].t.min(); a=d[d.msg=="SEND_ACKED"].t.min(); c=d[d.msg=="CRASH_POINT_HIT"].t.min()
    print(f.split("/")[-1],"first send->first ack ms",round((a-s).total_seconds()*1000,1),"; first send->crash ms",round((c-s).total_seconds()*1000,1))

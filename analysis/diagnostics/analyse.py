import pandas as pd, json, sys, datetime as dt
import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
S=sys.argv[1]
d=pd.read_csv(f"{S}/lat.csv"); d["sec"]=(d.start_ms//1000).astype(int)
g=d.groupby(d.sec//5*5).latency_ms.agg(n="count",p50="median",p99=lambda x:x.quantile(.99),max="max").round(1)
print(g.to_string())
print("slow(>50ms) by second:", d[d.latency_ms>50].groupby("sec").size().to_dict())
print("overall p50/p99 all:",d.latency_ms.quantile(.5).round(2),d.latency_ms.quantile(.99).round(2)," excluding first 5s:",d[d.sec>=5].latency_ms.quantile(.5).round(2),d[d.sec>=5].latency_ms.quantile(.99).round(2))
fig,ax=plt.subplots(figsize=(10,3.5)); ax.scatter(d.start_ms/1000,d.latency_ms,s=2); ax.set_yscale("log"); ax.set_xlabel("seconds"); ax.set_ylabel("latency ms"); fig.tight_layout(); fig.savefig(f"{S}/latency.png",dpi=110)
def load(f):
    out=[]
    for l in open(f):
        try:e=json.loads(l)
        except ValueError: continue
        if isinstance(e,dict) and "time" in e: e["t"]=pd.Timestamp(e["time"]); out.append(e)
    return out
L=load(f"{S}/lat.log"); P=[e for e in L if e["msg"]=="PRODUCE_REQUEST"]
p=pd.DataFrame(P); print("clean produce requests:",len(p),"total_ms p50/p99/max",p.total_ms.quantile(.5),p.total_ms.quantile(.99),p.total_ms.max())
sent={e["event_id"]:e["t"] for e in L if e["msg"]=="SENT_CALLED"}; ack=[(e["t"]-sent[e["event_id"]]).total_seconds()*1000 for e in L if e["msg"]=="SEND_ACKED" and e["event_id"] in sent]
print("clean sent->ack ms p50/p99/max",pd.Series(ack).quantile(.5).round(1),pd.Series(ack).quantile(.99).round(1),round(max(ack),1))
O=load(f"{S}/ovr.log")
sent={e["event_id"]:e for e in O if e["msg"]=="SENT_CALLED"}
acks=[(( e["t"]-sent[e["event_id"]]["t"]).total_seconds(),e) for e in O if e["msg"]=="SEND_ACKED" and e["event_id"] in sent]
acks.sort(key=lambda x:x[0]); w,e=acks[-1]
print(f"outage acked={len(acks)} failed={sum(1 for x in O if x['msg']=='SEND_FAILED')} slowest ack {w:.2f}s partition {e['partition']}; over 5s: {sum(1 for a,_ in acks if a>5)}")
s0=sent[e["event_id"]]["t"]; print("sent",s0.time(),"acked",e["t"].time())
for x in O:
    if x["msg"]=="PRODUCE_REQUEST" and s0-pd.Timedelta(seconds=1)<=x["t"]<=e["t"]+pd.Timedelta(seconds=1) and (x["total_ms"]>20 or x["write_err"]!="<nil>" or x["read_err"]!="<nil>"):
        print("  ",x["t"].time(),"total",x["total_ms"],"ms write_err",x["write_err"][:50],"read_err",x["read_err"][:60])
fails=[x["t"] for x in O if x["msg"]=="SEND_FAILED"]; print("first/last failure",fails[0].time(),fails[-1].time())
errs=pd.Series([x.get("error","")[:60] for x in O if x["msg"]=="SEND_FAILED"]).value_counts(); print(errs.to_string())
oth=[x for x in O if x["msg"] not in("SENT_CALLED","SEND_ACKED","SEND_FAILED","PRODUCE_REQUEST")]; print([x["msg"] for x in oth][:6])

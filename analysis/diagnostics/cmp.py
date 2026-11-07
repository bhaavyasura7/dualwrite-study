import pandas as pd,sys
S=sys.argv[1]
a=pd.read_csv(f"{S}/lat2.csv"); p=pd.read_csv(f"{S}/probe.csv",names=["t","ms"])
print("orders p50/p99/max",a.latency_ms.quantile(.5).round(1),a.latency_ms.quantile(.99).round(1),a.latency_ms.max().round(1))
print("healthz p50/p99/max",p.ms.quantile(.5).round(2),p.ms.quantile(.99).round(2),p.ms.max().round(1), "n",len(p))
s=a[a.latency_ms>50]; print("order slow secs:",s.groupby((s.start_ms//1000).astype(int)).latency_ms.agg(['count','max']).round(0).to_dict('index'))
q=p[p.ms>20]; print("healthz slow secs:",q.groupby((q.t//1000).astype(int)).ms.agg(['count','max']).round(0).to_dict('index'))

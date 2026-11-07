# Which partitions lost events in each C-2 cycle? Kafka's default partitioner is
# murmur2(key) & 0x7fffffff mod partitions, which franz-go also uses.
import json,sys,glob,os
import pandas as pd
def murmur2(data):
    m,r=0x5bd1e995,24; h=(0x9747b28c ^ len(data)) & 0xffffffff
    n=len(data)//4
    for i in range(n):
        k=int.from_bytes(data[4*i:4*i+4],'little'); k=(k*m)&0xffffffff; k^=k>>r; k=(k*m)&0xffffffff
        h=(h*m)&0xffffffff; h^=k
    rest=data[4*n:]
    if len(rest)>=3: h^=rest[2]<<16
    if len(rest)>=2: h^=rest[1]<<8
    if len(rest)>=1: h^=rest[0]; h=(h*m)&0xffffffff
    h^=h>>13; h=(h*m)&0xffffffff; h^=h>>15
    return h
part=lambda key:(murmur2(key.encode())&0x7fffffff)%3
T=sys.argv[1]
d=pd.read_csv(f"{T}/repC.csv")
for _,r in d.iterrows():
    f=f"{T}/damage/{r.cycle_id}.json"; lost=[]
    if os.path.exists(f): lost=json.load(open(f)).get("lost") or []
    c=pd.Series([part(x["order"]) for x in lost]).value_counts().to_dict()
    print(r.cycle_id[-3:],"lost",r.lost,"send_failed",r.send_failed,"by partition",{k:c.get(k,0) for k in range(3)})

# Reads a cycle topic with Kafka's own console consumer (all partitions, offset 0,
# read_committed) and plain JSON, then checks the checker's lost pairs are absent.
import json,subprocess,sys
cycle,damage=sys.argv[1],sys.argv[2]
out=subprocess.run(["docker","exec","dualwrite-broker-1-1","/opt/kafka/bin/kafka-console-consumer.sh",
  "--bootstrap-server","localhost:29091","--topic","orders."+cycle,"--from-beginning","--timeout-ms","15000",
  "--isolation-level","read_committed","--property","print.partition=true"],capture_output=True,text=True).stdout
pairs=set(); records=0; parts=set()
for line in out.splitlines():
    if not line.startswith("Partition:"): continue
    part,value=line.split("\t",1); parts.add(part); records+=1
    v=json.loads(value); a=v.get("after") or v
    pairs.add((a["order_id"], a.get("version", a.get("seq"))))
lost=[(x["order"],x["seq"]) for x in (json.load(open(damage)).get("lost") or [])]
print(cycle,"records",records,"partitions",len(parts),"distinct",len(pairs),"| checker lost",len(lost),"found in raw topic",sum(p in pairs for p in lost))

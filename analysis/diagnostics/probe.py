import http.client,time,sys
c=http.client.HTTPConnection("localhost",8080); out=open(sys.argv[1],"w"); end=time.time()+float(sys.argv[2]); t0=time.time()
while time.time()<end:
    a=time.perf_counter(); c.request("GET","/healthz"); c.getresponse().read(); b=time.perf_counter()
    out.write(f"{(time.time()-t0)*1000:.1f},{(b-a)*1000:.3f}\n"); time.sleep(0.005)

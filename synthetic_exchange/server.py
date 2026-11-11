"""合成协议服务；响应顺序由场景文件定义。"""
import argparse,json,base64,threading,urllib.parse
from pathlib import Path
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer

def make_server(scenario,port=0):
    counts={}; audit=[]; lock=threading.Lock()
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args): pass
        def do_POST(self): self.serve()
        def do_GET(self): self.serve()
        def serve(self):
            parsed=urllib.parse.urlsplit(self.path); query=urllib.parse.parse_qs(parsed.query)
            raw=self.rfile.read(int(self.headers.get('Content-Length','0')))
            if parsed.path=='/_requests':
                with lock: body=json.dumps(audit,ensure_ascii=False).encode()
                return self.reply(200,{'Content-Type':'application/json'},body)
            with lock:
                audit.append({'method':self.command,'path':self.path,'body':raw.decode('utf-8','replace'),
                              'range':self.headers.get('Range'),'if_range':self.headers.get('If-Range'),
                              'if_none_match':self.headers.get('If-None-Match')})
            if parsed.path=='/readings' and 'readings' in scenario:
                start=int(query.get('from',['0'])[0]); end=int(query.get('until',['0'])[0]); limit=scenario.get('limit',3)
                rows=[r for r in scenario['readings'] if start<=r['at']<end]
                return self.reply(200,{'Content-Type':'application/json'},json.dumps({'items':rows[:limit],'truncated':len(rows)>limit}).encode())
            candidates=[r for r in scenario['routes'] if r.get('method','GET')==self.command and r['path']==parsed.path and all(query.get(k)==[str(v)] for k,v in r.get('query',{}).items())]
            if not candidates: return self.reply(404,{},b'unknown local route')
            route=max(candidates,key=lambda r:len(r.get('query',{})))
            if any(self.headers.get(k)!=v for k,v in route.get('required_headers',{}).items()):return self.reply(403,{},b'local credential required')
            key=str(scenario['routes'].index(route))
            with lock:
                index=counts.get(key,0); counts[key]=index+1
            responses=route['responses']; value=responses[min(index,len(responses)-1)]
            headers=dict(value.get('headers',{})); status=value.get('status',200)
            if 'json' in value:
                body=json.dumps(value['json'],ensure_ascii=False).encode();headers.setdefault('Content-Type','application/json')
            elif 'base64' in value:body=base64.b64decode(value['base64'])
            else:body=value.get('body','').encode(value.get('encoding','utf-8'))
            if status==200 and headers.get('ETag') and self.headers.get('If-None-Match')==headers['ETag']:
                return self.reply(304,headers,b'')
            if route.get('ranges') and status==200 and self.headers.get('Range'):
                validator=self.headers.get('If-Range')
                if not validator or validator==headers.get('ETag'):
                    try:
                        spec=self.headers['Range'].removeprefix('bytes='); lo,hi=spec.split('-');lo=int(lo);hi=int(hi) if hi else len(body)-1
                        if lo<0 or lo>=len(body) or hi<lo: raise ValueError()
                        hi=min(hi,len(body)-1);total=len(body);body=body[lo:hi+1];status=206;headers['Content-Range']=f'bytes {lo}-{hi}/{total}'
                    except ValueError:return self.reply(416,{'Content-Range':f'bytes */{len(body)}'},b'')
            self.reply(status,headers,body,value.get('truncate'))
        def reply(self,status,headers,body,truncate=None):
            self.send_response(status)
            for key,value in headers.items():self.send_header(key,str(value))
            self.send_header('Content-Length',str(len(body)));self.end_headers()
            self.wfile.write(body if truncate is None else body[:truncate]);self.close_connection=True
    server=ThreadingHTTPServer(('127.0.0.1',port),Handler);server.daemon_threads=True
    return server

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--scenario',default='synthetic_exchange/scenario.json');parser.add_argument('--port',type=int,default=8765);args=parser.parse_args()
    srv=make_server(json.loads(Path(args.scenario).read_text()),args.port)
    print(json.dumps({'url':f'http://127.0.0.1:{srv.server_port}'}),flush=True)
    try:srv.serve_forever()
    except KeyboardInterrupt:pass
    finally:srv.server_close()

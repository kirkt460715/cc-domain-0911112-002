"""检查合成协议响应，避免联调服务本身发生错误。"""
import json,threading,unittest,urllib.request,urllib.error,urllib.parse
from pathlib import Path
from .server import make_server

class ProtocolChecks(unittest.TestCase):
    def test_declared_routes(self):
        scenario=json.loads((Path(__file__).parent/'scenario.json').read_text());server=make_server(scenario);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            base=f'http://127.0.0.1:{server.server_port}'
            for route in scenario['routes']:
                query=urllib.parse.urlencode(route.get('query',{}));url=base+route['path']+('?' + query if query else '')
                for expected in route['responses']:
                    request=urllib.request.Request(url,method=route.get('method','GET'),headers=route.get('required_headers',{}),data=b'{}' if route.get('method')=='POST' else None)
                    opener=urllib.request.build_opener(NoRedirect)
                    try:response=opener.open(request,timeout=3)
                    except urllib.error.HTTPError as e:response=e
                    self.assertEqual(response.status,expected.get('status',200))
                    if not expected.get('truncate'):
                        data=response.read()
                        if 'json' in expected:self.assertEqual(json.loads(data),expected['json'])
                        elif 'body' in expected:self.assertEqual(data,expected['body'].encode(expected.get('encoding','utf-8')))
                    response.close()
            if 'readings' in scenario:
                result=json.load(urllib.request.urlopen(base+'/readings?from=0&until=100',timeout=3))
                self.assertTrue(result['truncated']);self.assertEqual(len(result['items']),3)
            for route in scenario['routes']:
                if route.get('ranges') and route['path']=='/attachment':
                    request=urllib.request.Request(base+route['path'],headers={'Range':'bytes=0-7','If-Range':'"revision-1"'})
                    with urllib.request.urlopen(request,timeout=3) as response:
                        self.assertEqual(response.status,206);self.assertEqual(len(response.read()),8)
                if route['path']=='/article/a':
                    request=urllib.request.Request(base+route['path'],headers={'If-None-Match':'"a-v1"'})
                    with self.assertRaises(urllib.error.HTTPError) as caught:urllib.request.urlopen(request,timeout=3)
                    self.assertEqual(caught.exception.code,304)
            requests=json.load(urllib.request.urlopen(base+'/_requests',timeout=3));self.assertTrue(requests)
        finally:server.shutdown();server.server_close();thread.join(3)

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args):return None

if __name__=='__main__':unittest.main(verbosity=2)

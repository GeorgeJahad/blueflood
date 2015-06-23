import sys
sys.path.append('/Library/Python/2.7/site-packages')
from coverage import coverage
cov = coverage()
cov.start()

import time
import utils
import blueflood
import query
import unittest
import random
import grinder
from net.grinder.script.Grinder import grinder

try: 
  from com.xhaus.jyson import JysonCodec as json
except ImportError:
  import json
import pprint

pp = pprint.pprint
sleep_time = -1
get_url = None
post_url = None
post_payload = None

def mock_sleep(cls, x):
  global sleep_time
  sleep_time = x

class TestReq():
  def POST(self, url, payload):
    global post_url, post_payload
    post_url = url
    post_payload = payload
    return url, payload

  def GET(self, url):
    global get_url
    get_url = url
    return url

class BluefloodTests(unittest.TestCase):
  def setUp(self):
    self.real_shuffle = random.shuffle
    self.real_randint = random.randint
    self.real_time = utils.AbstractThread.time
    self.real_sleep = utils.AbstractThread.sleep
    self.tm = blueflood.ThreadManager(grinder)
    
    random.shuffle = lambda x: None
    random.randint = lambda x,y: 0
    utils.AbstractThread.time = lambda x:1
    utils.AbstractThread.sleep = mock_sleep
    test_config = {'report_interval': (1000 * 6),
                   'num_tenants': 3,
                   'metrics_per_tenant': 7,
                   'batch_size': 3,
                   'ingest_concurrency': 2,
                   'name_fmt': "int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.%d",
                   'num_nodes': 2}

    blueflood.default_config.update(test_config)

  def test_init_process(self):
    self.tm.create_all_metrics(0)
    self.assertEqual(blueflood.IngestThread.metrics,
                             [[[0, 0], [0, 1], [0, 2]],
                              [[0, 3], [0, 4], [0, 5]],
                              [[0, 6], [1, 0], [1, 1]],
                              [[1, 2], [1, 3], [1, 4]],
                              [[1, 5], [1, 6]]])
    

    thread = blueflood.IngestThread(0)
    self.assertEqual(thread.slice,
                             [[[0, 0], [0, 1], [0, 2]],
                              [[0, 3], [0, 4], [0, 5]],
                              [[0, 6], [1, 0], [1, 1]]])
    thread = blueflood.IngestThread(1)
    self.assertEqual(thread.slice,
                             [[[1, 2], [1, 3], [1, 4]], 
                              [[1, 5], [1, 6]]])

    self.assertEqual(query.QueryThread.total_queries, 30)
    self.assertEqual(query.QueryThread.num_queries_for_current_node, 15)

    thread = query.QueryThread(0)
    self.num_queries_for_current_thread = 2

    thread = query.QueryThread(4)
    self.num_queries_for_current_thread = 2

    self.tm.create_all_metrics(1)
    self.assertEqual(blueflood.IngestThread.metrics,
                             [[[2, 0], [2, 1], [2, 2]], 
                              [[2, 3], [2, 4], [2, 5]], 
                              [[2, 6]]])

    
    thread = blueflood.IngestThread(0)
    self.assertEqual(thread.slice,
                             [[[2, 0], [2, 1], [2, 2]], 
                              [[2, 3], [2, 4], [2, 5]]])
    thread = blueflood.IngestThread(1)
    self.assertEqual(thread.slice,
                             [[[2, 6]]])


    thread = query.QueryThread(0)
    self.num_queries_for_current_thread = 2

    thread = query.QueryThread(5)
    self.num_queries_for_current_thread = 1

  def test_generate_payload(self):
    self.tm.create_all_metrics(1)
    thread = blueflood.IngestThread(0)
    payload = json.loads(thread.generate_payload(0, [[2, 3], [2, 4], [2, 5]]))
    valid_payload = [{u'collectionTime': 0,
                      u'metricName': u'int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.3',
                      u'metricValue': 0,
                      u'tenantId': u'2',
                      u'ttlInSeconds': 172800,
                      u'unit': u'days'},
                     {u'collectionTime': 0,
                      u'metricName': u'int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.4',
                      u'metricValue': 0,
                      u'tenantId': u'2',
                      u'ttlInSeconds': 172800,
                      u'unit': u'days'},
                     {u'collectionTime': 0,
                      u'metricName': u'int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.5',
                      u'metricValue': 0,
                      u'tenantId': u'2',
                      u'ttlInSeconds': 172800,
                      u'unit': u'days'}]
    self.assertEqual(payload, valid_payload)

  def test_ingest_make_request(self):
    global sleep_time
    req = TestReq()
    thread = blueflood.IngestThread(0)
    thread.slice = [[[2, 0], [2, 1]]]
    thread.position = 0
    thread.finish_time = 10
    valid_payload = [{"collectionTime": 1, "ttlInSeconds": 172800, "tenantId": "2", "metricValue": 0, "unit": "days", "metricName": "int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.0"}, {"collectionTime": 1, "ttlInSeconds": 172800, "tenantId": "2", "metricValue": 0, "unit": "days", "metricName": "int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.1"}]

    url, payload = thread.make_request(pp, req)
    self.assertEqual(url, 
                     'http://qe01.metrics-ingest.api.rackspacecloud.com/v2.0/tenantId/ingest/multi')
    self.assertEqual(eval(payload), valid_payload)
    self.assertEqual(thread.position, 1)
    self.assertEqual(thread.finish_time, 10)
    thread.position = 2
    thread.make_request(pp, req)
    self.assertEqual(sleep_time, 9)
    self.assertEqual(thread.position, 1)
    self.assertEqual(thread.finish_time, 16)


  def test_query_make_request(self):
    req = TestReq()
    thread = query.QueryThread(0)
    thread.make_request(pp, req)
    self.assertEqual(get_url, "http://qe01.metrics.api.rackspacecloud.com/v2.0/0/views/int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.0?from=-86399999&to=1&resolution=FULL")

    random.randint = lambda x,y: 10
    thread.make_request(pp, req)
    self.assertEqual(get_url, "http://qe01.metrics.api.rackspacecloud.com/v2.0/10/metrics/search?query=int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.*")

    random.randint = lambda x,y: 20
    thread.make_request(pp, req)
    self.assertEqual(post_url, "http://qe01.metrics.api.rackspacecloud.com/v2.0/20/views?from=-86399999&to=1&resolution=FULL")
    self.assertEqual(eval(post_payload), ["int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.0","int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.1","int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.2","int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.3","int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.4","int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.5","int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.6","int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.7","int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.8","int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.9"])


  def tearDown(self):
    random.shuffle = self.real_shuffle
    random.randint = self.real_randint
    utils.AbstractThread.time = self.real_time
    utils.AbstractThread.sleep = self.real_sleep


#if __name__ == '__main__':
unittest.TextTestRunner().run(unittest.TestLoader().loadTestsFromTestCase(BluefloodTests))



cov.stop()
cov.save()
class TestRunner:
  def __init__(self):
    pass

  def __call__(self):
    pass

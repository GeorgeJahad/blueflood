import blueflood
import unittest
import random
import time
import json
import pprint

pp = pprint.pprint
sleep_time = -1

def mock_sleep(x):
  global sleep_time
  sleep_time = x

class TestReq():
  def POST(self, url, payload):
    return url, payload

class BluefloodTests(unittest.TestCase):
  def setUp(self):
    self.real_shuffle = random.shuffle
    self.real_randint = random.randint
    self.real_time = time.time
    self.real_sleep = time.sleep

    random.shuffle = lambda x: None
    random.randint = lambda x,y: 0
    time.time = lambda:1
    time.sleep = mock_sleep


    

  def test_init_process(self):
    test_config = {'report_interval': (1000 * 6),
                   'tenant_ids': 3,
                   'metrics_per_tenant': 7,
                   'batch_size': 3,
                   'concurrency': 2,
                   'num_nodes': 2}

    blueflood.default_config.update(test_config)
    batches = blueflood.init_process(0)
    self.assertSequenceEqual(batches,
                             [[[0, 0], [0, 1], [0, 2]],
                              [[0, 3], [0, 4], [0, 5]],
                              [[0, 6], [1, 0], [1, 1]],
                              [[1, 2], [1, 3], [1, 4]],
                              [[1, 5], [1, 6]]])

    
    current = blueflood.init_thread(0, batches)
    self.assertSequenceEqual(current['slice'],
                             [[[0, 0], [0, 1], [0, 2]],
                              [[0, 3], [0, 4], [0, 5]],
                              [[0, 6], [1, 0], [1, 1]]])
    current = blueflood.init_thread(1, batches)
    self.assertSequenceEqual(current['slice'],
                             [[[1, 2], [1, 3], [1, 4]], 
                              [[1, 5], [1, 6]]])

    batches = blueflood.init_process(1)
    self.assertSequenceEqual(batches,
                             [[[2, 0], [2, 1], [2, 2]], 
                              [[2, 3], [2, 4], [2, 5]], 
                              [[2, 6]]])

    
    current = blueflood.init_thread(0, batches)
    self.assertSequenceEqual(current['slice'],
                             [[[2, 0], [2, 1], [2, 2]], 
                              [[2, 3], [2, 4], [2, 5]]])
    current = blueflood.init_thread(1, batches)
    self.assertSequenceEqual(current['slice'],
                             [[[2, 6]]])


  def test_generate_payload(self):
    payload = json.loads(blueflood.generate_payload(0, [[2, 3], [2, 4], [2, 5]]))
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
    self.assertSequenceEqual(payload, valid_payload)

  def test_make_request(self):
    global sleep_time
    req = TestReq()
    current = {'slice': [[[2, 0], [2, 1]]],
          'position': 0,
          'finish_time': 10}
    url, payload = blueflood.make_request_for_this_thread(current, pp, req)
    self.assertEqual(url, 
                     'http://staging.metrics-ingest.api.rackspacecloud.com/v2.0/tenantId/ingest/multi')
    self.assertSequenceEqual(payload,
                             '[{"collectionTime": 1, "ttlInSeconds": 172800, "tenantId": "2", "metricValue": 0, "unit": "days", "metricName": "int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.0"}, {"collectionTime": 1, "ttlInSeconds": 172800, "tenantId": "2", "metricValue": 0, "unit": "days", "metricName": "int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.1"}]')
    self.assertEqual(current['position'], 1)
    self.assertEqual(current['finish_time'], 10)
    current['position'] = 2
    blueflood.make_request_for_this_thread(current, pp, req)
    self.assertEqual(sleep_time, 9)
    self.assertEqual(current['position'], 1)
    self.assertEqual(current['finish_time'], 16)


  def tearDown(self):
    random.shuffle = self.real_shuffle
    random.randint = self.real_randint
    time.time = self.real_time
    time.sleep = self.real_sleep

if __name__ == '__main__':
  unittest.main()



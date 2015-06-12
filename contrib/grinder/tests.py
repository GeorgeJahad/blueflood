import blueflood
import unittest
import random
import time
import pprint

pp = pprint.pprint
pp('hig')

class BluefloodTests(unittest.TestCase):
  def setUp(self):
    self.real_shuffle = random.shuffle
    self.real_randint = random.randint
    self.real_time = time.time

    random.shuffle = lambda x: None
    random.randint = lambda x,y: 0
    time.time = lambda:1


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

  def tearDown(self):
    random.shuffle = self.real_shuffle
    random.randint = self.real_randint
    time.time = self.real_time


if __name__ == '__main__':
  unittest.main()

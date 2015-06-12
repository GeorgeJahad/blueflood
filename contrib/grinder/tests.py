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
                   'batch_size': 5,
                   'concurrency': 2,
                   'num_nodes': 2}

    blueflood.default_config.update(test_config)
    batches = blueflood.init_process(1)
    pp(batches)
    batches = blueflood.init_process(1)
    current = blueflood.init_thread(0, batches)
    pp(current)

  def tearDown(self):
    random.shuffle = self.real_shuffle
    random.randint = self.real_randint
    time.time = self.real_time


if __name__ == '__main__':
  unittest.main()

import pprint
import time
import random
from net.grinder.script.Grinder import grinder

pp = pprint.pprint

default_config = {
  'name_fmt': "t4.int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.%d",
  'report_interval': (1000 * 10),
  'num_tenants': 4,
  'metrics_per_tenant': 15,
  'batch_size': 5,
  'ingest_concurrency': 15,
  'num_nodes': 1,
  'url': "http://localhost:19000",
  'query_url': "http://localhost:20000",
  'query_concurrency': 10,
  'max_multiplot_metrics': 10,
  'search_queries_per_interval': 10,
  'multiplot_per_interval': 10,
  'singleplot_per_interval': 10}

units_map = {0: 'minutes',
             1: 'hours',
             2: 'days',
             3: 'months',
             4: 'years',
             5: 'decades'}


RAND_MAX =  982374239

class ThreadManager(object):
  types = []


  @classmethod
  def add_type(cls, type):
    cls.types.append(type)

  def convert(self, s):
    try:
      return int(s)
    except:
      return eval(s)

  def setup_config(self, grinder):
    for entry in grinder.getProperties().entrySet():       
      if not entry.key.startswith("grinder.bf."):
        continue
      k = entry.key.replace("grinder.bf.","")
      default_config[k] = self.convert(entry.value)

  def __init__(self, grinder):
    self.setup_config(grinder)

  def create_all_metrics(self, agent_number):
    for x in self.types:
      x.create_metrics(agent_number)

  def setup_thread(self, thread_num):
    thread_type = None
    server_num = thread_num

    for x in self.types:
      if server_num < x.num_threads():
        thread_type = x
        break
      else:
        server_num -= x.num_threads()

    if thread_type == None:
      print "gbjerror ", thread_num, self.types
      raise Exception("Invalid Thread Type")

    return thread_type(thread_num)


class AbstractThread(object):
  @classmethod
  def create_metrics(cls, agent_number):
    raise Exception("Can't create abstract thread")

  @classmethod
  def num_threads(cls):
    raise Exception("Can't create abstract thread")

  def make_request(self, logger, request):
    raise Exception("Can't create abstract thread")

  def __init__(self, thread_num):
    self.position = 0
    self.finish_time = int(self.time()) + (default_config['report_interval'] / 1000)

  @classmethod
  def generate_job_range(cls, total_jobs, total_servers, server_num):
    jobs_per_server = total_jobs/total_servers
    remainder = total_jobs % total_servers
    start_job = jobs_per_server * server_num
    start_job += min(remainder, server_num)
    end_job = start_job + jobs_per_server
    if server_num < remainder:
      end_job += 1
    return (start_job, end_job)

  @classmethod
  def generate_metrics_tenants(cls, num_tenants, metrics_per_tenant, 
                               agent_number, num_nodes, gen_fn):
    tenants_in_shard = range(*cls.generate_job_range(num_tenants, num_nodes, agent_number))
    metrics = []
    for y in map(lambda x: gen_fn(x, metrics_per_tenant), tenants_in_shard):
      metrics += y
    random.shuffle(metrics)
    return metrics

  def generate_metric_name(self, metric_id):
    return default_config['name_fmt'] % metric_id

  def generate_unit(self, tenant_id):
    unit_number = tenant_id % 6
    return units_map[unit_number]

  def check_position(self, logger, max_position):
    if self.position >= max_position:
      self.position = 0
      sleep_time = self.finish_time - int(self.time())
      self.finish_time += (default_config['report_interval'] / 1000)
      if sleep_time < 0:
        #return error
        logger("finish time error")
      else:
        logger("pausing for %d" % sleep_time)
        self.sleep(sleep_time)

  @classmethod
  def time(cls):
    return time.time()

  @classmethod
  def sleep(cls, x):
    return time.sleep(x)

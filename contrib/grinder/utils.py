import pprint
import time
import random

pp = pprint.pprint

qe01_config = {
  'name_fmt': "t4.int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.%d",
  'report_interval': (1000 * 6),
  'num_tenants': 4,
  'metrics_per_tenant': 10,
  'batch_size': 5,
  'ingest_concurrency': 1,
  'num_nodes': 1,
  'url': "http://qe01.metrics-ingest.api.rackspacecloud.com",
  'query_url': "http://qe01.metrics.api.rackspacecloud.com",
  'query_concurrency': 100,
  'search_queries_per_interval': 10,
  'multiplot_per_interval': 10,
  'singleplot_per_interval': 10}

stage_config = {
  'name_fmt': "int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.%d",
  'report_interval': (1000 * 60),
  'num_tenants': 23000,
  'metrics_per_tenant': 210,
  'batch_size': 1500,
  'ingest_concurrency': 25,
  'num_nodes': 1,
  'url':  "http://staging.metrics-ingest.api.rackspacecloud.com",
  'query_url':  "http://staging.metrics.api.rackspacecloud.com",
  'query_concurrency': 50,
  'search_queries_per_interval': 100,
  'multiplot_per_interval': 20,
  'singleplot_per_interval': 300}

units_map = {0: 'minutes',
             1: 'hours',
             2: 'days',
             3: 'months',
             4: 'years',
             5: 'decades'}


default_config = qe01_config

RAND_MAX =  982374239

class ThreadManager(object):
  types = []
  total_threads = 0

  @classmethod
  def add_type(cls, type):
    cls.types.append(type)
    cls.total_threads += type.num_threads()

  def prn_types(self):
    print self.types

  def create_all_metrics(self, agent_number):
    for x in self.types:
      x.create_metrics(agent_number)

  def setup_thread(self, thread_num, grinder):
    thread_type = None
    server_num = thread_num

    for x in self.types:
      if server_num < x.num_threads():
        thread_type = x
        break
      else:
        server_num -= x.num_threads()

    if thread_type == None:
      raise Exception("Invalid Thread Type")

    return thread_type(thread_num)


class AbstractThread(object):
  @classmethod
  def create_metrics(cls, agent_number):
    raise("Can't create abstract thread")

  @classmethod
  def num_threads(cls):
    raise("Can't create abstract thread")

  def make_request(self, logger, request):
    raise("Can't create abstract thread")

  def __init__(self, thread_num):
    self.position = 0
    self.finish_time = int(time.time()) + (default_config['report_interval'] / 1000)

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
      sleep_time = self.finish_time - int(time.time())
      self.finish_time += (default_config['report_interval'] / 1000)
      if sleep_time < 0:
        #return error
        logger("finish time error")
      else:
        logger("pausing for %d" % sleep_time)
        time.sleep(sleep_time)

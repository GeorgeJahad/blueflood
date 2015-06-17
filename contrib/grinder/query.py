import random
try: 
  from com.xhaus.jyson import JysonCodec as json
except ImportError:
  import json
import time
from utils import *

class QueryThread(AbstractThread):
  num_queries_for_current_node = 0
  total_queries = 0
  query_types = ('search_queries_per_interval',
                 'singleplot_per_interval',
                 'multiplot_per_interval')

  @classmethod
  def create_metrics(cls, agent_number):
    for x in cls.query_types:
      cls.total_queries += default_config[x]

    start_job, end_job = cls.generate_job_range(cls.total_queries, 
                                                default_config['num_nodes'], agent_number)
    cls.num_queries_for_current_node = end_job - start_job

  @classmethod
  def num_threads(cls):
    return default_config['query_concurrency']

  def __init__(self, thread_num):
    AbstractThread.__init__(self, thread_num)
    start_query, end_query = self.generate_job_range(self.num_queries_for_current_node,
                                                                  ThreadManager.total_threads,
                                                                  thread_num)
    self.num_queries_for_current_thread = end_query - start_query

  def get_query_type(self):
    num = random.randint(0, self.total_queries)
    for x in self.query_types:
      if num < default_config[x]:
        return x
      num -= default_config[x]

    raise("Invalid query type")

  def generate_singleplot(self, time):
    tenant_id = random.randint(0, default_config['num_tenants'])
    metric_name = self.generate_metric_name(random.randint(0, default_config['metrics_per_tenant']))
    to = time
    from = time - (* 1000 60 60 24)
    resolution = 'FULL'
    url = xxxyy

  def generate_payload(self, time):
    payload = map(lambda x:self.generate_metric(time,*x), batch)
    return url,json.dumps(payload)

  def make_request(self, logger, request_handler):
    self.check_position(logger, self.num_queries_for_current_thread)
    url, payload = self.generate_payload(int(time.time()))
    self.position += 1
    result = request_handler.POST(url, payload)
    return result

ThreadManager.add_type(QueryThread)


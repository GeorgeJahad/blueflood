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
  queries_per_intervals = ('singleplot_per_interval', 
                       'search_queries_per_interval', 'multiplot_per_interval')
  @classmethod
  def create_metrics(cls, agent_number):
    for q in cls.queries_per_intervals:
      cls.total_queries += default_config[q]
      print "gbjc", cls.total_queries

    start_job, end_job = cls.generate_job_range(cls.total_queries, 
                                                default_config['num_nodes'], agent_number)
    cls.num_queries_for_current_node = end_job - start_job


  @classmethod
  def num_threads(cls):
    return default_config['query_concurrency']

  def __init__(self, thread_num):
    AbstractThread.__init__(self, thread_num)
    self.query_fns = {'singleplot_per_interval': self.generate_singleplot,
                      'search_queries_per_interval': self.generate_search,
                      'multiplot_per_interval': self.generate_multiplot}

    start_query, end_query = self.generate_job_range(self.num_queries_for_current_node,
                                                                  ThreadManager.total_threads,
                                                                  thread_num)
    self.num_queries_for_current_thread = end_query - start_query

  def get_query_fn(self):
    num = random.randint(0, self.total_queries)
    for q in self.queries_per_intervals:
      if num < default_config[q]:
        return self.query_fns[q]
      num -= default_config[q]
                                                                  
    raise Exception("Invalid query type")

  def generate_multiplot_payload():
    metrics_count = min(default_config['max_multiplot_metrics'], 
                        random.randint(0, default_config['metrics_per_tenant']))
    metrics_list = map(generate_metric_name, range(metrics_count))


  def generate_multiplot(self, time, logger, request_handler):
    pass

  def generate_search(self, time, logger, request_handler):
    pass

  def generate_singleplot(self, time, logger, request_handler):
    tenant_id = random.randint(0, default_config['num_tenants'])
    metric_name = self.generate_metric_name(random.randint(0, default_config['metrics_per_tenant']))
    to = time
    frm = time - (1000 * 60 * 60 * 24)
    resolution = 'FULL'
    url =  "%s/v2.0/%d/views/%s?from=%d&to=%s&resolution=%s" % (default_config['query_url'],
                                                                tenant_id, metric_name, frm,
                                                                to, resolution)
    result = request_handler.GET(url)
    logger(result.getText())

  def make_request(self, logger, request_handler):
    self.check_position(logger, self.num_queries_for_current_thread)
    result = (self.get_query_fn())(int(time.time()), logger, request_handler)
    self.position += 1
    return result

ThreadManager.add_type(QueryThread)


import itertools, random
try: 
  from com.xhaus.jyson import JysonCodec as json
except ImportError:
  import json
import time
from utils import *

class QueryThread(AbstractThread):
  num_queries_for_current_node = []

  @classmethod
  def create_metrics(cls, agent_number):
    total_queries = (default_config['search_queries_per_interval'] + 
                                default_config['singleplot_per_interval'] + 
                                default_config['singleplot_per_interval']) 
    start_job, end_job = cls.generate_job_range(total_queries, 
                                                default_config['num_nodes'], agent_number)
    cls.num_queries_for_current_node = end_job - start_job

  @classmethod
  def num_threads(cls):
    return default_config['query_concurrency']

  def __init__(self, thread_num):
    start_query, end_query = self.generate_job_range(self.num_queries_for_current_node,
                                                                  ThreadManager.total_threads,
                                                                  thread_num)
    self.num_queries_for_current_thread = end_query - start_query

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


import itertools, random
try: 
  from com.xhaus.jyson import JysonCodec as json
except ImportError:
  import json
import time
from utils import *

class QueryThread(AbstractThread):
  batches = []
  @classmethod
  def create_batches(cls, agent_number):
    pass

  def num_threads(cls):
    return default_config['query_concurrency']

  @classmethod
  def generate_metrics_for_tenant(cls, tenant_id, metrics_per_tenant):
    l = [];
    for x in range(metrics_per_tenant):
      l.append([tenant_id, x])
    return l

  def generate_payload(self, time, batch):
    payload = map(lambda x:self.generate_metric(time,*x), batch)
    return json.dumps(payload)

  def query_url(self):
    return "%s/v2.0/tenantId/ingest/multi" % default_config['query_url']


  def make_request(self, logger, request_handler):
    self.check_position(logger)
    payload = self.generate_payload(int(time.time()),
                                           self.slice[self.position])
    self.position += 1
    result = request_handler.POST(self.ingest_url(), payload)
    return result

ThreadManager.add_type(QueryThread)


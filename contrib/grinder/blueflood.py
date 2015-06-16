import itertools, random
try: 
  from com.xhaus.jyson import JysonCodec as json
except ImportError:
  import json
import time
from utils import *

class IngestThread(ThreadType):
  batches = []
  @classmethod
  def create_batches(cls, agent_number):
    cls.batches =  cls.generate_metrics_tenants(default_config['batch_size'], 
                                            default_config['tenant_ids'], 
                                            default_config['metrics_per_tenant'], agent_number, 
                                            default_config['num_nodes'])

  def __init__(self, thread_num):
    start, end = self.generate_job_range(len(self.batches), 
                                    default_config['ingest_concurrency'], thread_num)
    self.slice = self.batches[start:end]
    self.position = 0
    self.finish_time = int(time.time()) + (default_config['report_interval'] / 1000)

  def num_threads(self):
    return default_config['ingest_concurrency']

  def generate_metric_name(self, metric_id):
    return default_config['name_fmt'] % metric_id

  def generate_unit(self, tenant_id):
    unit_number = tenant_id % 6
    return units_map[unit_number]

  @classmethod
  def divide_batches(cls, metrics, batch_size):
    b = []
    for i in range(0, len(metrics), batch_size):
      b.append(metrics[i:i+batch_size])
    return b

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
  def generate_metrics_tenants(cls, batch_size, tenant_ids, metrics_per_tenant, agent_number, num_nodes):
    def generate_metrics_for_tenant(tenant_id):
      l = [];
      for x in range(metrics_per_tenant):
        l.append([tenant_id, x])
      return l
    tenants_in_shard = range(*cls.generate_job_range(tenant_ids, num_nodes, agent_number))
    metrics = []
    for y in map(generate_metrics_for_tenant, tenants_in_shard):
      metrics += y
    random.shuffle(metrics)
    return cls.divide_batches(metrics, batch_size)

  def generate_metric(self, time, tenant_id, metric_id):
    return {'tenantId': str(tenant_id),
            'metricName': self.generate_metric_name(metric_id),
            'unit': self.generate_unit(tenant_id),
            'metricValue': random.randint(0, RAND_MAX),
            'ttlInSeconds': (2 * 24 * 60 * 60),
            'collectionTime': time}

  def generate_payload(self, time, batch):
    payload = map(lambda x:self.generate_metric(time,*x), batch)
    return json.dumps(payload)

  def ingest_url(self):
    return "%s/v2.0/tenantId/ingest/multi" % default_config['url']


  def make_request(self, logger, request_handler):
    if self.position >= len(self.slice):
      self.position = 0
      sleep_time = self.finish_time - int(time.time())
      self.finish_time += (default_config['report_interval'] / 1000)
      if sleep_time < 0:
        #return error
        logger("finish time error")
      else:
        logger("pausing for %d" % sleep_time)
        time.sleep(sleep_time)
    payload = self.generate_payload(int(time.time()),
                                           self.slice[self.position])
    self.position += 1
    result = request_handler.POST(self.ingest_url(), payload)
    return result

ThreadManager.add_type(IngestThread)

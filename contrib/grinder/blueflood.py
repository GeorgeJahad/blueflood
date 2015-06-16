import itertools, random
try: 
  from com.xhaus.jyson import JysonCodec as json
except ImportError:
  import json
import time
import pprint

pp = pprint.pprint

qe01_config = {
  'name_fmt': "t4.int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.%d",
  'report_interval': (1000 * 6),
  'tenant_ids': 4,
  'metrics_per_tenant': 10,
  'batch_size': 5,
  'ingest_concurrency': 1,
  'num_nodes': 1,
  'url': "http://qe01.metrics-ingest.api.rackspacecloud.com",
  'query_url': "http://qe01.metrics.api.rackspacecloud.com",
  'query_concurrency': 100,
  'search_queries_per_minute': 10,
  'multiplot_per_minute': 10,
  'singleplot_per-minute': 10}

stage_config = {
  'name_fmt': "int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.%d",
  'report_interval': (1000 * 60),
  'tenant_ids': 23000,
  'metrics_per_tenant': 210,
  'batch_size': 1500,
  'ingest_concurrency': 25,
  'num_nodes': 1,
  'url':  "http://staging.metrics-ingest.api.rackspacecloud.com",
  'query_url':  "http://staging.metrics.api.rackspacecloud.com",
  'query_concurrency': 50,
  'search_queries_per_minute': 100,
  'multiplot_per_minute': 20,
  'singleplot_per-minute': 300}

default_config = stage_config

RAND_MAX =  982374239

class ThreadManager(object):
  types = []

  @classmethod
  def add_type(cls, type):
    cls.types.append(type)

  def prn_types(self):
    print self.types

  def create_all_batches(self, agent_number):
    for x in self.types:
      x.create_batches(agent_number)

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


class ThreadType(object):
  @classmethod
  def create_batches(cls, agent_number):
    pass

  def num_threads(self):
    pass

  def make_request(self, logger, request):
    pass


class IngestThread(ThreadType):
  batches = []

  @classmethod
  def create_batches(cls, agent_number):
    cls.batches =  generate_metrics_tenants(default_config['batch_size'], 
                                            default_config['tenant_ids'], 
                                            default_config['metrics_per_tenant'], agent_number, 
                                            default_config['num_nodes'])

  def __init__(self, thread_num):
    start, end = generate_job_range(len(self.batches), 
                                    default_config['ingest_concurrency'], thread_num)
    self.slice = batches[start:end]
    self.position = 0
    self.finish_time = int(time.time()) + (default_config['report_interval'] / 1000)

  def num_threads(self):
    return default_config['ingest_concurrency']

def generate_metric_name(metric_id):
  return default_config['name_fmt'] % metric_id

units_map = {0: 'minutes',
             1: 'hours',
             2: 'days',
             3: 'months',
             4: 'years',
             5: 'decades'}

def generate_unit(tenant_id):
  unit_number = tenant_id % 6
  return units_map[unit_number]

def divide_batches(metrics, batch_size):
  batches = []
  for i in range(0, len(metrics), batch_size):
    batches.append(metrics[i:i+batch_size])
  return batches

def generate_job_range(total_jobs, total_servers, server_num):
  jobs_per_server = total_jobs/total_servers
  remainder = total_jobs % total_servers
  start_job = jobs_per_server * server_num
  start_job += min(remainder, server_num)
  end_job = start_job + jobs_per_server
  if server_num < remainder:
    end_job += 1
  return (start_job, end_job)

def generate_metrics_tenants(batch_size, tenant_ids, metrics_per_tenant, agent_number, num_nodes):
  def generate_metrics_for_tenant(tenant_id):
    l = [];
    for x in range(metrics_per_tenant):
      l.append([tenant_id, x])
    return l
  tenants_in_shard = range(*generate_job_range(tenant_ids, num_nodes, agent_number))
  metrics = []
  for y in map(generate_metrics_for_tenant, tenants_in_shard):
    metrics += y
  random.shuffle(metrics)
  return divide_batches(metrics, batch_size)

def generate_metric(time, tenant_id, metric_id):
  return {'tenantId': str(tenant_id),
          'metricName': generate_metric_name(metric_id),
          'unit': generate_unit(tenant_id),
          'metricValue': random.randint(0, RAND_MAX),
          'ttlInSeconds': (2 * 24 * 60 * 60),
          'collectionTime': time}

def generate_payload(time, batch):
  payload = map(lambda x:generate_metric(time,*x), batch)
  return json.dumps(payload)

def ingest_url():
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
  payload = generate_payload(int(time.time()),
                                         self.slice[self.position])
  self.position += 1
  result = request_handler.POST(ingest_url(), payload)
  return result

ThreadManager.add_type(IngestThread)

  

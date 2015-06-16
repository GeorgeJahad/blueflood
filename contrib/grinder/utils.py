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

  @classmethod
  def num_threads(cls):
    pass

  def make_request(self, logger, request):
    pass



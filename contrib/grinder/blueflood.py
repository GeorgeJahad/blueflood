import itertools, random
try: 
  from com.xhaus.jyson import JysonCodec as json
except ImportError:
  import json

default_config = {'report_interval': (1000 * 6),
                  'tenant_ids': 23000,
                  'metrics_per_tenant': 210,
                  'batch_size': 1000,
                  'concurrency': 25,
                  'offset': 0,
                  'num_instances': 1,
                  'url': "http://qe01.metrics-ingest.api.rackspacecloud.com",
                  'query_url': "http://qe01.metrics.api.rackspacecloud.com",
                  'query_concurrency': 100,
                  'search_queries_per_minute': 10,
                  'multiplot_per_minute': 10,
                  'singleplot_per-minute': 10}

RAND_MAX =  982374239

name_fmt = "int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.%d"

def generate_metric_name(metric_id):
  return name_fmt % metric_id

units_map = {0: "minutes",
             1: "hours",
             2: "days",
             3: "months",
             4: "years",
             5: "decades"}

def generate_unit(tenant_id):
  unit_number = tenant_id % 6
  return units_map[unit_number]

def create_batches(metrics, batch_size):
  batches = []
  for i in range(0, len(metrics), batch_size):
    batches.append(metrics[i:i+batch_size])
  return batches

def generate_metrics_tenants(batch_size, tenant_ids, metrics_per_tenant, offset, num_instances):
  def generate_metrics_for_tenant(tenant_id):
    l = [];
    for x in range(metrics_per_tenant):
      l.append([tenant_id, x])
    return l
  tenant_shard = tenant_ids / num_instances
  max_tenant_in_shard = min(tenant_ids,(offset + 1) * tenant_shard)
  tenants_in_shard = range((offset * tenant_shard), max_tenant_in_shard)
  metrics = []
  for y in map(generate_metrics_for_tenant, tenants_in_shard):
    metrics += y
  random.shuffle(metrics)
  return create_batches(metrics, batch_size)


def generate_metric(time, tenant_id, metric_id):
  return {'tenantId': str(tenant_id),
          'metricName': generate_metric_name(metric_id),
          'unit': generate_unit(tenant_id),
          'metricValue': random.randint(0, RAND_MAX),
          'ttlInSeconds': (2 * 24 * 60 * 60),
          'collectionTime': time}

def generate_payload(time, batch):
  return json.dumps(map(lambda x:generate_metric(time,*x), batch))

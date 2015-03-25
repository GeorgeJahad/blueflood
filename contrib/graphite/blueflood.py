from __future__ import absolute_import

import re
import time
import requests
import json
import auth
import sys
import traceback
import remote_pdb
import os.path
import itertools

try:
    from graphite_api.intervals import Interval, IntervalSet
    from graphite_api.node import LeafNode, BranchNode
except ImportError:
    from graphite.intervals import Interval, IntervalSet
    from graphite.node import LeafNode, BranchNode

# curl -XPOST -H "Accept: application/json, text/plain, */*" -H "Content-Type: application/x-www-form-urlencoded" 'http://127.0.0.1:8888/render' -d "target=rackspace.*.*.*.*.*.*.*.*.available&from=-6h&until=now&format=json&maxDataPoints=1552"


secs_per_res = {
  'FULL': 1,
  'MIN5': 5*60,
  'MIN20': 20*60,
  'MIN60': 60*60,
  'MIN240': 240*60,
  'MIN1440': 1440*60}

def calc_res(start, stop):
  # make an educated guess about the likely number of data points returned.
  num_points = (stop - start) / 60
  res = 'FULL'
  if num_points > 60:
    num_points = (stop - start) / secs_per_res['MIN5']
    res = 'MIN5'
  if num_points > 800:
    num_points = (stop - start) / secs_per_res['MIN20']
    res = 'MIN20'
  if num_points > 800:
    num_points = (stop - start) / secs_per_res['MIN60']
    res = 'MIN60'
  if num_points > 800:
    num_points = (stop - start) / secs_per_res['MIN240']
    res = 'MIN240'
  if num_points > 800:
    num_points = (stop - start) / secs_per_res['MIN1440']
    res = 'MIN1440'
  return res

def grouper(iterable, n, fillvalue=None):
  "Collect data into fixed-length chunks or blocks"
  # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
  args = [iter(iterable)] * n
  return itertools.izip_longest(fillvalue=fillvalue, *args)



class TenantBluefloodFinder(object):
  __fetch_multi__ = 'tenant_blueflood'
  def __init__(self, config=None):
    print("gbj v5")
    if os.path.isfile("/root/pdb-flag"):
      remote_pdb.RemotePdb('127.0.0.1', 4444).set_trace()
    authentication_module = None
    if config is not None:
      if 'urls' in config['blueflood']:
        urls = config['blueflood']['urls']
      else:
        urls = [config['blueflood']['url'].strip('/')]
      tenant = config['blueflood']['tenant']
      if 'authentication_module' in config['blueflood']:
        authentication_module = config['blueflood']['authentication_module']
        authentication_class = config['blueflood']['authentication_class']
    else:
      from django.conf import settings
      urls = getattr(settings, 'BF_QUERY')
      tenant = getattr(settings, 'BF_TENANT')
      authentication_module = getattr(settings, 'BF_AUTHENTICATION_MODULE', None)
      authentication_class = getattr(settings, 'BF_AUTHENTICATION_CLASS', None)

    if authentication_module:
      module = __import__(authentication_module)
      class_ = getattr(module, authentication_class)
      bfauth = class_(config)
      auth.set_auth(bfauth)

    self.tenant = tenant
    self.bf_query_endpoint = urls[0]


  def find_nodes(self, query):
    try:
      query_depth = len(query.pattern.split('.'))
      #print 'DAS QUERY ' + str(query_depth) + ' ' + query.pattern
      client = Client(self.bf_query_endpoint, self.tenant)
      values = client.find_metrics(query.pattern)
      
      for obj in values:
        metric = obj['metric']
        parts = metric.split('.')
        metric_depth = len(parts)
        if metric_depth > query_depth:
          yield BranchNode('.'.join(parts[:query_depth]))
        else:
          yield LeafNode(metric, TenantBluefloodReader(metric, self.tenant, self.bf_query_endpoint))
    except Exception as e:
     print "Exception in Blueflood find_nodes: " 
     print e
     exc_info = sys.exc_info()
     tb = traceback.format_exception(*exc_info)
     for line in tb:
       print(line)
     raise e

  def get_metric_list(self, endpoint, tenant, metric_list, payload, headers):
    url = "%s/v2.0/%s/views" % (endpoint, tenant)
    if auth.is_active():
      headers['X-Auth-Token'] = auth.get_token(False)
    r = requests.post(url, params=payload, data=json.dumps(metric_list), headers=headers)
    if r.status_code == 401 and auth.is_active():
      headers['X-Auth-Token'] = auth.get_token(True)
      r = requests.post(url, params=payload, data=json.dumps(metric_list), headers=headers)
    if r.status_code != 200:
      print("gbj bad response: ", r.status_code, tenant, metric_list)
      return None
    else:
      #print("gbj met: ", r.json()['metrics'])
      return r.json()['metrics']

  def fetch_multi(self, nodes, start_time, end_time):
    paths = [node.path for node in nodes]
    res = calc_res(start_time, end_time)
    step = secs_per_res[res]
    payload = {
      'from': start_time * 1000,
      'to': end_time * 1000,
      'resolution': res
    }
    headers = auth.headers()
    real_end_time = end_time + step
    # Limit size MPlot requests
    groups = [filter(None,l) for l in list(grouper(paths, 40, False))]
    responses = reduce(lambda x,y: x+y,
                         [self.get_metric_list(self.bf_query_endpoint, 
                                               self.tenant, g, payload, headers) 
                          for g in groups])
    cache = {x['metric'] : client.process_path(x['data'], start_time, real_end_time, step)
             for x in responses}
    time_info = (start_time, real_end_time, step)
    return (time_info, cache)

class TenantBluefloodReader(object):
  __slots__ = ('metric', 'tenant', 'bf_query_endpoint')
  supported = True

  def __init__(self, metric, tenant, endpoint):
    # print 'READER ' + tenant + ' ' + metric
    self.metric = metric
    self.tenant = tenant
    self.bf_query_endpoint = endpoint

  def get_intervals(self):
    # todo: make this a lot smarter.
    intervals = []
    millis = int(round(time.time() * 1000))
    intervals.append(Interval(0, millis))
    return IntervalSet(intervals)

  def fetch(self, start_time, end_time):
    # remember, graphite treats time as seconds-since-epoch. BF treats time as millis-since-epoch.
    if not self.metric:
      return ((start_time, end_time, 1), [])
    else:
      res = calc_res(start_time, end_time)
      step = secs_per_res[res]
      client = Client(self.bf_query_endpoint, self.tenant)
      values = client.get_values(self.metric, start_time, end_time, res)
      real_end_time = end_time + step
      processed_values = client.process_path(values, start_time, real_end_time, step)
      time_info = (start_time, real_end_time, step)
      print("gbj4 ", time_info, len(processed_values))
      if len(processed_values) > 0:
        print("gbj44 ", processed_values[0])
      return (time_info, processed_values)

class Client(object):
  def __init__(self, host, tenant):
    self.host = host
    self.tenant = tenant

  def make_request(self, url, payload, headers):
    if auth.is_active():
      headers['X-Auth-Token'] = auth.get_token(False)
    r = requests.get(url, params=payload, headers=headers)
    if r.status_code == 401 and auth.is_active():
      headers['X-Auth-Token'] = auth.get_token(True)
      r = requests.get(url, params=payload, headers=headers)
    return r


  def find_metrics(self, query):
    payload = {'query': query}
    headers = auth.headers()
    r = self.make_request("%s/v2.0/%s/metrics/search" % (self.host, self.tenant), payload, headers)
    if r.status_code != 200:
      print str(r.status_code) + ' in find_metrics ' + r.url + ' ' + r.text
      return []
    else:
      try:
        return r.json()
      except TypeError:
        # we need to parse the json ourselves.
        return json.loads(r.text)
      except ValueError:
        return ['there was an error']

  def get_values(self, metric, start, stop, res):
    payload = {
      'from': start * 1000,
      'to': stop * 1000,
      'resolution': res
    }
    #print 'USING RES ' + res
    headers = auth.headers()

    r = self.make_request("%s/v2.0/%s/views/%s" % (self.host, self.tenant, metric), payload, headers)
    #print("gbj request %s/v2.0/%s/views/%s" % (self.host, self.tenant, metric), payload, headers, json.loads(r.text)['values'])
    if r.status_code != 200:
      print str(r.status_code) + ' in get_values ' + r.text
      return {'values': []}
    else:
      try:
        return r.json()['values']
      except TypeError:
        # parse that json yo
        return json.loads(r.text)['values']
      except ValueError:
        print 'ValueError in get_values'
        return {'values': []}

  def current_datapoint_valid(self, v_iter, data_key, ts, step):
    if (not len(v_iter)) or (not (data_key in v_iter[0])):
      return False
    datapoint_ts = v_iter[0]['timestamp']/1000
    if (ts <= datapoint_ts) and (datapoint_ts < (ts + step)):
      return True
    return False
    
  

  def process_path(self, values, start_time, end_time, step):
    value_res_order = ['average', 'latest', 'numPoints']
    present_keys = [_ for _ in value_res_order if _ in values[0]]
    if present_keys:
      data_key = present_keys[0]

    v_iter = values
    ret_arr = []
    for ts in range(start_time, end_time, step):
      if self.current_datapoint_valid(v_iter, data_key, ts, step):
        ret_arr.append(v_iter[0][data_key])
      else:
        ret_arr.append(None)
      # We only want one datapoint for each step
      while self.current_datapoint_valid(v_iter, 'timestamp', ts, step):
        v_iter = v_iter[1:]
      
    return ret_arr

class TenantBluefloodLeafNode(LeafNode):
  __fetch_multi__ = 'tenant_blueflood'



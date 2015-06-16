import itertools, random
try: 
  from com.xhaus.jyson import JysonCodec as json
except ImportError:
  import json
import time
from utils import *

class QueryThread(ThreadType):
  batches = []
  @classmethod
  def create_batches(cls, agent_number):
    pass

  def num_threads(cls):
    return default_config['query_concurrency']



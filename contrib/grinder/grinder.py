# A more complex HTTP example based on an authentication conversation
# with the server. This script demonstrates how to follow different
# paths based on a response returned by the server and how to post
# HTTP form data to a server.
#
# The J2EE Servlet specification defines a common model for form based
# authentication. When unauthenticated users try to access a protected
# resource, they are challenged with a logon page. The logon page
# contains a form that POSTs username and password fields to a special
# j_security_check page.
 
from net.grinder.script.Grinder import grinder
from net.grinder.script import Test
from net.grinder.plugin.http import HTTPRequest
from HTTPClient import NVPair
import blueflood
import time
 
request = HTTPRequest()
batches = blueflood.init_process(grinder.getAgentNumber())
 
class TestRunner:
  def __init__(self):
    self.current = blueflood.init_thread(grinder.getThreadNumber(), batches)

  def __call__(self):
    if self.current['first']:
      self.current['first'] = False
      self.current['finish_time'] = time.time() + (blueflood.default_config['report_interval'] / 1000)
    if self.current['position'] >= len(self.current['slice']):
      self.current['position'] = 0
      sleep_time = self.current['finish_time'] - time.time()
      self.current['finish_time'] += (blueflood.default_config['report_interval'] / 1000)
      if sleep_time < 0:
        #return error
        grinder.info.logger("finish time error")
      else:
        grinder.info.logger("pausing for %d" % sleep_time)
        time.sleep(sleep_time)

    payload = blueflood.generate_payload(time.time(), 
                                         self.current['slice'][self.current['position']])
            
    grinder.logger.info(payload)
    self.current['position'] += 1
    result = request.POST(blueflood.ingest_url(), payload)

    grinder.logger.info("gbjdone2")
    grinder.logger.info(result.toString())

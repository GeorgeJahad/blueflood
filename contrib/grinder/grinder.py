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



#protectedResourceTest = Test(1, "Request resource")
#authenticationTest = Test(2, "POST to j_security_check")
 
request = HTTPRequest()
#protectedResourceTest.record(request)
 
class TestRunner:
    def __call__(self):
      c = blueflood.default_config
      batches = blueflood.generate_metrics_tenants(c['batch_size'], c['tenant_ids'], c['metrics_per_tenant'], c['offset'], c['num_instances'])
      payload = blueflood.generate_payload(1432551600000, batches[0])

      grinder.logger.info(payload)
      result = request.POST("http://qe01.metrics-ingest.api.rackspacecloud.com/v2.0/939271/ingest/multi", payload)

      grinder.logger.info("gbjdone2")
      grinder.logger.info(result.toString())

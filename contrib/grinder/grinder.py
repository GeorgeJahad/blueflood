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
from com.xhaus.jyson import JysonCodec as json
#import blueflood
#import json



#protectedResourceTest = Test(1, "Request resource")
#authenticationTest = Test(2, "POST to j_security_check")
 
#request = HTTPRequest()
#protectedResourceTest.record(request)
 
class TestRunner:
    def __call__(self):
#      result = request.POST("http://qe01.metrics-ingest.api.rackspacecloud.com/v2.0/939271/ingest", "[{\"collectionTime\": 1428541532428, \"ttlInSeconds\": 172800, \"metricValue\": 66, \"metricName\": \"rackspace.example.metric.two22gbj\"}]")

      grinder.logger.info("gbjdone")
      grinder.logger.info(json.dumps({'1': 2}))
#      grinder.logger.info(result.getStatusCode())
#      grinder.logger.info(result.toString())

# #        result = request.GET()
#         result = maybeAuthenticate(resul# A more complex HTTP example based on an authentication conversation
# with the server. This script demonstrates how to follow different
# paths based on a response returned by the server and how to post
# HTTP form data to a server.
#
# The J2EE Servlet specification defines a common model for form based
# authentication. When unauthenticated users try to access a protected
# resource, they are challenged with a logon page. The logon page
# contains a form that POSTs username and password fields to a special
# j_security_check page.

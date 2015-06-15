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
 
test1 = Test(1, "Ingest resource")
request = HTTPRequest()
blueflood.ThreadType.create_all_batches(grinder.getAgentNumber())
test1.record(request)
 
class TestRunner:
  def __init__(self):
    self.thread = blueflood.ThreadType.setup_thread(grinder.getThreadNumber(), grinder)
    runs = grinder.getProperties().getInt("grinder.runs",-1)

  def __call__(self):
    result = self.thread.make_request(grinder.logger.info, request)


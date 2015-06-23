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
import blueflood
import query

#The code inside the class is gets executed by each worker thread
#Outside the class is executed before any of the workers begin
thread_manager = blueflood.ThreadManager(grinder)
thread_manager.create_all_metrics(grinder.getAgentNumber())
 
class TestRunner:
  def __init__(self):
    self.thread = thread_manager.setup_thread(grinder.getThreadNumber())

  def __call__(self):
    result = self.thread.make_request(grinder.logger.info)


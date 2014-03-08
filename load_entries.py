import sys,os
sys.path.insert(0, os.environ.get('GOOGLE_APPENGINE_SDK_PATH'))
sys.path.insert(0, os.environ.get('GOOGLE_APPENGINE_NDB_PATH'))
import dev_appserver
dev_appserver.fix_sys_path()
from google.appengine.ext import testbed
t = testbed.Testbed()
os.environ['APPLICATION_ID'] = 'foo'
t.activate()
t.init_datastore_v3_stub(use_sqlite=True, datastore_file='100GzipText.sqlite')
t.init_memcache_stub()
import ndb
class Entity(ndb.Model):
  val = ndb.TextProperty(compressed=True)
for i in range(100):
  e = Entity()
  e.val = 'a' * 10000
  e.put()
t.deactivate()
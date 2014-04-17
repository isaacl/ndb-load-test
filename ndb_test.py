import sys, os
sys.path.insert(0, os.environ.get('GOOGLE_APPENGINE_SDK_PATH'))
sys.path.insert(0, os.environ.get('GOOGLE_APPENGINE_NDB_PATH'))
from google.appengine.dist27 import threading
import thread
sys.modules.pop('google', None)
import dev_appserver
dev_appserver.fix_sys_path()
from google.appengine.ext import testbed
from guppy import hpy
from meliae import scanner
import logging
import time
logging.basicConfig(level=5,
                    #format='%(levelname)-8s %(message)s',
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%H:%M:%S',
                    filename='/tmp/ndb.%d.log' % int(time.time()),
                    filemode='w')
t = testbed.Testbed()
os.environ['APPLICATION_ID'] = 'foo'
t.activate()
t.init_datastore_v3_stub(use_sqlite=True, datastore_file='100GzipText.sqlite')
t.init_memcache_stub()
import ndb
class NEntity(ndb.Model):
  pass
class Entity(ndb.Model):
  val = ndb.TextProperty(compressed=True)
hp = hpy()
import gc, inspect, weakref, pprint
from pprint import pprint as pp
wd = weakref.WeakValueDictionary()
ndb.get_context().set_cache_policy(False)
#ndb.get_context().set_memcache_policy(False)
hp.setref()

def mem():
  t = 0
  for l in open('/proc/%d/smaps' % os.getpid()):
    if l.startswith('Private'):
      t += int(l.split()[1])
  return t

tids = {}
futs = weakref.WeakKeyDictionary()

def run_once():
  wd = weakref.WeakValueDictionary()
  s_mem = mem()
  def f():
    for i, ent in enumerate(Entity.query().iter(batch_size=10)):
      wd[i] = ent
      if i == 95:
        break
    NEntity().put()
    tid = tids.setdefault(thread.get_ident(), len(tids))
    futs.update((fut, tid) for fut in ndb.tasklets._state.all_pending)
  t = threading.Thread(target=f)
  t.start(); t.join()
  gc.collect()
  time.sleep(1)
  n_mem = mem()
  return n_mem, n_mem - s_mem, len(wd)

def fetch_entities(paged=False, batch_size=10):
  if paged:
    def fetcher():
      cursor = None
      while True:
        entities, cursor, more = Entity.query().fetch_page(batch_size, start_cursor=cursor)
        for e in entities:
          yield e
        if not more:
          return
    fetch_it = fetcher()
  else:
    fetch_it = Entity.query().iter(batch_size=batch_size)
  total_txt = 0
  for i, entity in enumerate(fetch_it):
    wd[i] = entity
    total_txt += len(entity.val)
    if i == 95:
      gc.collect()
      return i, total_txt, len(wd)

    #if i % (batch_size // 2) == batch_size//4:
    #  gc.collect()
    #  if len(wd) > batch_size * 2:
    #    return i, total_txt

  gc.collect()

def ppn(es):
  ans = list(es.nodes)
  for e in ans[:10]:
    print pprint.pformat(e)[:150]
  if len(ans) > 10:
    print '...', len(ans) - 10
def g1(es):
  return iter(es.nodes).next()

def print_gens(gens):
  return pp([(g.gi_code,
           [t for t in inspect.getmembers(g.gi_frame)
            if t[0] in ('f_locals', 'f_lineno')])
          for g in gens.nodes])

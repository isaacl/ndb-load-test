import thread
import gc
import hashlib
import itertools
import logging
import pprint
import time
import weakref
import os

import webapp2

from google.appengine.api import runtime
from google.appengine.api import taskqueue
from google.appengine.ext import ndb


class Entity(ndb.Model):
  text = ndb.TextProperty(compressed=True)

class Stat(ndb.Expando):
  end_time = ndb.DateTimeProperty(auto_now_add=True)

class BaseHandler(webapp2.RequestHandler):
  def logger(self):
    self.start_mem = runtime.memory_usage().current()
    self.start_t = time.time()
    logger = logging.getLogger(self.__class__.__name__)
    sh = logging.StreamHandler(self.response)
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    formatter.formatTime = lambda _, *__: '%.2f %.2f' % (
        time.time() - self.start_t, runtime.memory_usage().current() - self.start_mem)
    logger.root.handlers[0].setFormatter(formatter)
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    self.response.headers['Content-Type'] = 'text/plain'
    return logger

def md5(*data):
  h = hashlib.md5()
  for d in data:
    h.update(repr(d))
  return h.hexdigest()

class CreateData(BaseHandler):
  def get(self):
    log = self.logger()
    ent_cls = self.request.get('ents', '1000')
    tt = 'a' * int(self.request.get('chars', 25000))
    cur_h = time.time()
    for i in xrange(int(ent_cls) // 100):
      es = []
      for j in xrange(100):
        cur_h = md5(cur_h)
        es.append(Entity(key=ndb.Key('k', ent_cls, 'e', i * 100 + j + 1, Entity, cur_h), text=tt))
      pprint.pprint(es)
      ndb.put_multi(es)
      log.info('round %d', i)

def get_stat(request, run=None):
  md5_h = md5(request.url, time.time())
  run = run or request.get('run')
  stat = Stat(key=ndb.Key('id', run, Stat, md5_h[:15]))
  stat.opt_batch_size = int(request.get('batch_size', 100))
  stat.opt_use_gc = bool(int(request.get('use_gc', 1)))
  stat.opt_paged = bool(int(request.get('paged', 0)))
  stat.opt_ent_class = request.get('ent_cls', '1000')
  stat.cutoff = int(request.get('cutoff', int(stat.opt_ent_class) * 0.95))
  return stat

def query(logger, stat, start_t, start_mem):
  stat.instance = md5(os.environ.get('INSTANCE_ID'))[:10]
  os.environ['QUERY_NUM'] = q_num = os.environ.get('QUERY_NUM', 0) + 1
  stat.thread = '%d:%d' % (thread.get_ident(), q_num)
  gc.collect()
  ndb.get_context().set_cache_policy(False)
  #logger.info(stat)
  ancestor = ndb.Key('k', stat.opt_ent_class)
  if stat.opt_paged:
    def fetcher():
      cursor = None
      while True:
        entities, cursor, more = Entity.query(
            ancestor=ancestor).fetch_page(stat.opt_batch_size, start_cursor=cursor)
        for e in entities:
          yield e
        if not more:
          return
    fetch_it = fetcher()
  else:
    fetch_it = Entity.query(ancestor=ancestor).iter(batch_size=stat.opt_batch_size)

  chars = i = 0
  wd = weakref.WeakValueDictionary()
  for i, entity in enumerate(fetch_it):
    chars += len(entity.text or '')
    wd[i] = entity
    #if i % 200 == 0:
    #  logger.info('%d', i)
    if i > stat.cutoff:
      break
    #if i == 5000:
    #  import pdb; pdb.set_trace()
    #  #from meliae import scanner
    #  #scanner.dump_all_objects('/tmp/maliae/%d_%s_%d_%s_mem.dat' % (stat.batch_size, stat.paged, i, stat.gc))

  entity = None
  if stat.opt_use_gc:
    gc.collect()
  stat.ents_in_mem = len(wd)
  # logging.info(pprint.pformat(filter(bool,
  #   sorted([getattr(e._result, 'key', None) for e in getattr(fu, '_queue', [])] for fu in ndb.tasklets._state.all_pending))))
  stat.ents_fetched = i + 1
  stat.chars_fetched = chars
  stat.pending_rpcs = len(ndb.eventloop._state.event_loop.rpcs)
  stat.pending_futures = len(ndb.tasklets._state.all_pending)
  stat.runtime = time.time() - start_t
  time.sleep(2)
  stat.start_mem = start_mem
  stat.net_mem = runtime.memory_usage().current() - start_mem
  logger.info(stat)
  stat.put()

class TaskHandler(BaseHandler):
  def get(self):
    run_id = str(time.time())
    if self.request.get('do_run'):
      i = 0
      for tup in itertools.product([10, 200, 1000], [1050, 9990, 19990, 29990]):
        params = dict(zip(['batch_size', 'cutoff'], tup))
        params['run'] = run_id
        params['ent_cls'] = self.request.get('ent_cls', '1000')
        for bsize in [10, 200, 1000]:
          params['batch_size'] = bsize
          taskqueue.Queue().add(taskqueue.Task(
              url='/task/query', params=params,
              retry_options=taskqueue.TaskRetryOptions(task_age_limit=5*60)))
          i += 1
      self.response.headers['Content-Type'] = 'text/plain'
      self.response.write('pushed %d tasks, https://ls.googleplex.com/stats?id=%s' % (i, run_id))
    else:
      logger = self.logger()
      stat = get_stat(self.request, run_id)
      query(logger, stat, self.start_t, self.start_mem)
      self.response.write(pprint.pformat(stat.to_dict()))
  def post(self):
    logger = self.logger()
    stat = get_stat(self.request)
    query(logger, stat, self.start_t, self.start_mem)

def human_print(lines):
  for line in lines:
    assert len(line) == len(lines[0])
  num_cols = len(lines[0])
  format_string = ''
  str_lines = [tuple(str(e) for e in line) for line in lines]
  for col in xrange(num_cols - 1):
    col_width = max(len(line[col]) for line in str_lines) + 4
    format_string += '%-' + str(col_width) + 's '
  format_string += '%s'
  return [format_string % line for line in str_lines]

class Stats(webapp2.RequestHandler):
  def get(self):
    r_id = self.request.get('id')
    ancestor = ndb.Key('id', r_id) if r_id else None
    stat_dicts = [e.to_dict() for e in Stat.query(ancestor=ancestor)]
    if not stat_dicts:
      return
    cols = sorted(
        set(itertools.chain(*stat_dicts)),
        key=lambda c: (not c.startswith('opt'), 'pending' in c, 'fetched' in c, 'mem' in c))

    stat_dicts.sort(key=lambda r: (r.get('ents_in_mem'), r.get('net_mem')), reverse=True)
    table = [cols] + [[r.get(c) for c in cols] for r in stat_dicts]
    self.response.headers['Content-Type'] = 'text/plain'
    if self.request.get('c', 0) == 0:
      self.response.write('\n'.join(human_print(table)))
    else:
      self.response.write('\n'.join(','.join(str(c) for c in r) for r in table))


app = webapp2.WSGIApplication([
    ('/create', CreateData),
    ('/task/query', TaskHandler),
    ('/stats', Stats)
    ], debug=True)

import os
from google.appengine.ext import ndb
from google.appengine.api import taskqueue
from google.appengine.api import runtime

import gc
import itertools
import logging
import time
import urlparse
import webapp2
import hashlib
import pprint

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


class Entity(ndb.Model):
    text = ndb.TextProperty()

class Stat(ndb.Model):
  run = ndb.DateTimeProperty(auto_now_add=True)
  batch_size = ndb.IntegerProperty()
  paged = ndb.BooleanProperty()
  ctx_cache = ndb.BooleanProperty()
  gc = ndb.BooleanProperty()
  runtime = ndb.FloatProperty()
  total_mem = ndb.FloatProperty()
  entities = ndb.IntegerProperty()


class BaseHandler(webapp2.RequestHandler):
  def __init__(self, *args, **kwargs):
    return super(BaseHandler, self).__init__(*args, **kwargs)

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
    tt = 'a' * 25000
    cur_h = time.time()
    for i in xrange(10):
      es = []
      for _ in xrange(100):
        cur_h = md5(cur_h, time.time())
        es.append(Entity(key=ndb.Key('k', '1000', Entity, cur_h), text=tt))
      ndb.put_multi(es)
      log.info('round %d', i)

def get_stat(request, run=None):
  md5_h = md5(request.url, time.time())
  run = run or handle.request.get('run')
  stat = Stat(key=ndb.Key('id', run, Stat, md5_h[:15]))
  stat.batch_size = int(request.get('batch_size', 100))
  stat.ctx_cache = bool(int(request.get('no_cache', 0)))
  stat.gc = bool(int(request.get('enable_gc', 0))) 
  stat.paged = bool(int(request.get('use_page', 0)))
  return stat

def query(logger, stat, start_t, start_mem):
  gc.collect()
  if stat.ctx_cache:
    ndb.get_context().set_cache_policy(False)

  logger.info(stat)
  ancestor = ndb.Key('k', '1000')
  if not stat.paged:
    fetch_it = Entity.query(ancestor=ancestor).iter(batch_size=stat.batch_size)
  else:
    def fetcher():
      cursor = None
      while True:
        entities, cursor, more = Entity.query(ancestor=ancestor).fetch_page(stat.batch_size, start_cursor=cursor)
        for e in entities:
          yield e
        if not more:
          return
    fetch_it = fetcher()

  i = 0
  for i, entity in enumerate(fetch_it):
    _ = entity.text
    if i % 200 == 0:
      logger.info('%d', i)
    ndb.get_context().clear_cache()
    if stat.gc:
      gc.collect()

  stat.entities = i
  stat.total_mem = runtime.memory_usage().current() - start_mem
  stat.runtime = time.time() - start_t
  stat.put()


class TaskHandler(BaseHandler):
  def get(self):
    run_id = str(time.time())
    if self.request.get('do_run'):
      i=0
      for tup in itertools.product(range(2), repeat=3):
        params = dict(zip(['no_cache', 'use_page', 'enable_gc'], tup))
        params['run'] = run_id
        for bsize in [10, 75, 85, 200, 1000]:
          params['batch_size'] = bsize
          taskqueue.Queue().add(taskqueue.Task(
              url='/task/query', params=params,
              retry_options=taskqueue.TaskRetryOptions(task_age_limit=5*60)))
          i+=1
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

class Stats(webapp2.RequestHandler):
  def get(self):
    r_id = self.request.get('id')
    ancestor = ndb.Key('id', r_id) if r_id else None
    stat_dicts = [e.to_dict() for e in Stat.query(ancestor=ancestor)]
    if not stat_dicts:
      return
    cols = sorted(set(itertools.chain(*stat_dicts)))
    table = [cols] + sorted([[r.get(c) for c in cols] for r in stat_dicts], key=lambda r: r[-1])
    self.response.headers['Content-Type'] = 'text/plain'
    if self.request.get('n', 0) != 0:
      self.response.write('\n'.join(human_print(table)))
    else:
      self.response.write('\n'.join(','.join(str(c) for c in r) for r in table))


app = webapp2.WSGIApplication([
    ('/create', CreateData),
    ('/task/query', TaskHandler),
    ('/stats', Stats)
    ], debug=True)

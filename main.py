import os
from google.appengine.ext import ndb
from google.appengine.api import taskqueue
from google.appengine.api import runtime

import time
import webapp2
import logging
import gc

class Entity(ndb.Model):
    text = ndb.TextProperty()

class BaseHandler(webapp2.RequestHandler):
  def logger(self):
    logger = logging.getLogger(self.__class__.__name__)
    start_t = time.time()
    start_mem = runtime.memory_usage().current()
    sh = logging.StreamHandler(self.response)
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    formatter.formatTime = lambda _, *__: '%.2f %.2f' % (
        time.time() - start_t, runtime.memory_usage().current() - start_mem)
    logger.root.handlers[0].setFormatter(formatter)
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    self.response.headers['Content-Type'] = 'text/plain'
    return logger.info

class CreateData(BaseHandler):
  def get(self):
    log = self.logger()
    tt = 'a' * 25000
    for i in xrange(10):
      ndb.put_multi(Entity(text=tt) for _ in xrange(1000))
      log('round %d', i)

    
class TaskHandler(BaseHandler):
  def get(self):
    return taskqueue.Queue().add(taskqueue.Task(
        url='/task/fetch?%s' % self.request.query_string,
        retry_options=taskqueue.TaskRetryOptions(task_retry_limit=0, task_age_limit=3*60)))

  def post(self):
    log = self.logger()
    entity_query = Entity.query()
    no_cache = bool(self.request.get('no_cache'))
    enable_gc = bool(self.request.get('enable_gc'))
    if no_cache:
      ndb.get_context().set_cache_policy(False)

    batch_size = int(self.request.get('batch_size', 100))
    use_page = bool(self.request.get('use_page'))
    log('batch %d, page %s, no_cache %s, gc %s', batch_size, use_page, no_cache, enable_gc)
    if use_page:
      fetch_it = Entity.query().iter(batch_size=batch_size)
    else:
      def fetcher():
        cursor = None
        while True:
          entities, cursor, more = entity_query.fetch_page(batch_size, start_cursor=cursor)
          for e in entities:
            yield e
          if not more:
            return
      fetch_it = fetcher()

    for i, entity in enumerate(fetch_it):
      _ = entity.text
      if i % 1000 == 0:
        log('%d', i)
      ndb.get_context().clear_cache()
      if enable_gc:
        gc.collect()

app = webapp2.WSGIApplication([
    ('/create', CreateData),
    ('/query', TaskHandler)
    ], debug=True)

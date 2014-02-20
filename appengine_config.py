
appstats_MAX_STACK = 20
appstats_RECORD_FRACTION = 1.0

def webapp_add_wsgi_middleware(app):
  from google.appengine.ext.appstats import recording
  app = recording.appstats_wsgi_middleware(app)
  return app

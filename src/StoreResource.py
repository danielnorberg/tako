from twisted.web import resource
import Store
import simplejson as json

class StoreResource(resource.Resource):
	isLeaf = True

	def __init__(self):
		self.store = Store.Store()

	def render_GET(self):
		"""docstring for render_GET"""
		key = '/'.join(request.postpath)
		value = self.store.get(key)
		if value:
			request.setHeader("Content-Type", "application/json; charset=utf-8")
			return json.dumps(value)
		else:
			request.setResponseCode(http.NOT_FOUND)
			return ''

	def render_POST(self):
		"""docstring for render_POST"""
		key = '/'.join(request.postpath)
		value=json.loads(request.args['value'][0])
		self.store.set(key, value)
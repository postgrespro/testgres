import unittest
from testgres import get_new_node, clean_all

class SimpleTest(unittest.TestCase):

	def teardown(self):
		# clean_all()
		pass

	def test_start_stop(self):
		assert(1)
		node = get_new_node('test')
		node.init()
		node.start()
		res = node.execute('postgres', 'select 1')
		self.assertEqual(len(res), 1)
		self.assertEqual(res[0][0], 1)
		node.stop()

if __name__ == '__main__':
	unittest.main()

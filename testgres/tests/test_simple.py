import unittest
from testgres import get_new_node, clean_all

class SimpleTest(unittest.TestCase):

	def teardown(self):
		# clean_all()
		pass

	@unittest.skip("demo")
	def test_start_stop(self):
		node = get_new_node('test')
		node.init()
		node.start()
		res = node.execute('postgres', 'select 1')
		self.assertEqual(len(res), 1)
		self.assertEqual(res[0][0], 1)
		node.stop()

	def test_backup(self):
		node = get_new_node('test')
		replica = get_new_node('repl')

		node.init(allows_streaming=True)
		node.start()
		node.psql('postgres', 'create table abc(a int, b int)')
		node.psql('postgres', 'insert into abc values (1, 2)')
		node.backup('my_backup')

		replica.init_from_backup(node, 'my_backup')
		replica.start()
		res = replica.execute('postgres', 'select * from abc')
		self.assertEqual(len(res), 1)
		self.assertEqual(res[0], (1, 2))

		node.stop()

if __name__ == '__main__':
	unittest.main()

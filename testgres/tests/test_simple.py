import unittest
import time
from testgres import get_new_node, clean_all, stop_all

class SimpleTest(unittest.TestCase):

	def teardown(self):
		# clean_all()
		stop_all()

	@unittest.skip("demo")
	def test_start_stop(self):
		node = get_new_node('test')
		node.init()
		node.start()
		res = node.execute('postgres', 'select 1')
		self.assertEqual(len(res), 1)
		self.assertEqual(res[0][0], 1)
		node.stop()

	def test_backup_and_replication(self):
		node = get_new_node('test')
		replica = get_new_node('repl')

		node.init(allows_streaming=True)
		node.start()
		node.psql('postgres', 'create table abc(a int, b int)')
		node.psql('postgres', 'insert into abc values (1, 2)')
		node.backup('my_backup')

		replica.init_from_backup(node, 'my_backup', has_streaming=True)
		replica.start()
		res = replica.execute('postgres', 'select * from abc')
		self.assertEqual(len(res), 1)
		self.assertEqual(res[0], (1, 2))

		# Insert into master node
		node.psql('postgres', 'insert into abc values (3, 4)')
		# Wait until data syncronizes
		node.poll_query_until(
			'postgres',
			'SELECT pg_current_xlog_location() <= write_location '
			'FROM pg_stat_replication WHERE application_name = \'%s\''
			% replica.name)
		# time.sleep(0.5)
		# Check that this record was exported to replica
		res = replica.execute('postgres', 'select * from abc')
		self.assertEqual(len(res), 2)
		self.assertEqual(res[1], (3, 4))

		node.stop()
		replica.stop()

if __name__ == '__main__':
	unittest.main()

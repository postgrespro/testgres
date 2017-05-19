#!/usr/bin/env python

import unittest
import re
import six
import tempfile
import logging.config

from testgres import get_new_node, stop_all, clean_all


class SimpleTest(unittest.TestCase):

    def teardown(self):
        clean_all()
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
            'SELECT pg_current_xlog_location() <= replay_location '
            'FROM pg_stat_replication WHERE application_name = \'%s\''
            % replica.name)
        # time.sleep(0.5)
        # Check that this record was exported to replica
        res = replica.execute('postgres', 'select * from abc')
        self.assertEqual(len(res), 2)
        self.assertEqual(res[1], (3, 4))

        node.stop()
        replica.stop()

    def test_dump(self):
        node = get_new_node('test')
        node.init().start()
        node.safe_psql(
            'postgres',
            'create table abc as '
            'select g as a, g as b from generate_series(1, 10) as g'
        )
        node.psql('postgres', 'create database test')
        node.dump('postgres', 'test.sql')
        node.restore('test', 'test.sql')
        self.assertEqual(
            node.psql('postgres', 'select * from abc'),
            node.psql('test', 'select * from abc'),
        )
        node.stop()

    def test_users(self):
        node = get_new_node('master')
        node.init().start()
        node.psql('postgres', 'create role test_user login')
        value = node.safe_psql('postgres', 'select 1', username='test_user')
        self.assertEqual(value, six.b('1\n'))
        node.stop()

    def test_logging(self):
        regex = re.compile('\w+:\s{1}LOG:.*')
        logfile = tempfile.NamedTemporaryFile('w', delete=True)

        log_conf = {
            'version': 1,
            'handlers': {
                'file': {
                    'class': 'logging.FileHandler',
                    'filename': logfile.name,
                    'formatter': 'base_format',
                    'level': logging.DEBUG,
                },
            },
            'formatters': {
                'base_format': {
                    'format': '%(node)-5s: %(message)s',
                },
            },
            'root': {
                'handlers': ('file', ),
                'level': 'DEBUG',
            },
        }

        logging.config.dictConfig(log_conf)

        node = get_new_node('master', use_logging=True)
        node1 = get_new_node('slave1', use_logging=True)
        node2 = get_new_node('slave2', use_logging=True)

        node.init().start()
        node1.init().start()
        node2.init().start()

        with open(logfile.name, 'r') as log:
            for line in log:
                self.assertTrue(regex.match(line))

        node.stop()
        node1.stop()
        node2.stop()


if __name__ == '__main__':
    unittest.main()

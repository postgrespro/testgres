#!/usr/bin/env python

import unittest
import re
import six
import tempfile
import logging.config

from testgres import get_new_node, stop_all, get_config, clean_all, bound_ports


class SimpleTest(unittest.TestCase):
    def teardown(self):
        clean_all()
        stop_all()

    @unittest.skip("demo")
    def test_start_stop(self):
        with get_new_node('test') as node:
            node.init().start()
            res = node.execute('postgres', 'select 1')
            self.assertEqual(len(res), 1)
            self.assertEqual(res[0][0], 1)

    def test_backup_and_replication(self):
        with get_new_node('test') as node, get_new_node('repl') as replica:
            node.init(allows_streaming=True)
            node.start()
            node.psql('postgres', 'create table abc(a int, b int)')
            node.psql('postgres', 'insert into abc values (1, 2)')
            node.backup('my_backup')

            replica.init_from_backup(node, 'my_backup', has_streaming=True)
            replica.start()
            res = replica.execute('postgres', 'select * from abc')
            self.assertEqual(len(res), 1)
            self.assertEqual(res[0][0], 1)
            self.assertEqual(res[0][1], 2)

            # Prepare the query which would check whether record reached replica
            # (It is slightly different for Postgres 9.6 and Postgres 10+)
            if get_config()['VERSION_NUM'] >= 1000000:
                wait_lsn = 'SELECT pg_current_wal_lsn() <= replay_lsn '           \
                    'FROM pg_stat_replication WHERE application_name = \'%s\''    \
                    % replica.name
            else:
                wait_lsn = 'SELECT pg_current_xlog_location() <= replay_location '\
                    'FROM pg_stat_replication WHERE application_name = \'%s\''    \
                    % replica.name

            # Insert into master node
            node.psql('postgres', 'insert into abc values (3, 4)')
            # Wait until data syncronizes
            node.poll_query_until('postgres', wait_lsn)
            # Check that this record was exported to replica
            res = replica.execute('postgres', 'select * from abc')
            self.assertEqual(len(res), 2)
            self.assertEqual(res[1][0], 3)
            self.assertEqual(res[1][1], 4)

    def test_dump(self):
        with get_new_node('test') as node:
            node.init().start()
            node.safe_psql(
                'postgres', 'create table abc as '
                'select g as a, g as b from generate_series(1, 10) as g')
            node.psql('postgres', 'create database test')
            node.dump('postgres', 'test.sql')
            node.restore('test', 'test.sql')
            self.assertEqual(
                node.psql('postgres', 'select * from abc'),
                node.psql('test', 'select * from abc'), )

    def test_users(self):
        with get_new_node('master') as node:
            node.init().start()
            node.psql('postgres', 'create role test_user login')
            value = node.safe_psql('postgres', 'select 1', username='test_user')
            self.assertEqual(value, six.b('1\n'))

    def test_logging(self):
        regex = re.compile('.+?LOG:.*')
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

        with get_new_node('master', use_logging=True) as node, \
            get_new_node('slave1', use_logging=True) as node1, \
            get_new_node('slave2', use_logging=True) as node2:

            node.init().start()
            node1.init().start()
            node2.init().start()

            with open(logfile.name, 'r') as log:
                for line in log:
                    self.assertTrue(regex.match(line))

    def test_ports_management(self):
        # check that no ports have been bound yet
        self.assertEqual(len(bound_ports), 0)

        with get_new_node('node') as node:
            # check that we've just bound a port
            self.assertEqual(len(bound_ports), 1)

            # check that bound_ports contains our port
            port_1 = list(bound_ports)[0]
            port_2 = node.port
            self.assertEqual(port_1, port_2)

        # check that port has been freed successfuly
        self.assertEqual(len(bound_ports), 0)


if __name__ == '__main__':
    unittest.main()

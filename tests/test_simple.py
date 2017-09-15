#!/usr/bin/env python

import unittest
import re
import six
import tempfile
import logging.config
import subprocess

from distutils.version import LooseVersion

from testgres import InitNodeException, \
    StartNodeException, ExecUtilException, \
    BackupException, QueryException

from testgres import get_new_node, get_pg_config
from testgres import bound_ports
from testgres import NodeStatus


class SimpleTest(unittest.TestCase):
    def test_double_init(self):
        with get_new_node('test') as node:
            got_exception = False

            try:
                node.init()
                node.init()
            except InitNodeException as e:
                got_exception = True
            except Exception as e:
                pass

        self.assertTrue(got_exception)

    def test_uninitialized_start(self):
        with get_new_node('test') as node:
            got_exception = False

            try:
                node.start()
            except StartNodeException as e:
                got_exception = True
            except Exception as e:
                pass

        self.assertTrue(got_exception)

    def test_restart(self):
        with get_new_node('test') as node:
            node.init().start()
            res = node.execute('postgres', 'select 1')
            self.assertEqual(res, [(1, )])
            node.restart()
            res = node.execute('postgres', 'select 2')
            self.assertEqual(res, [(2, )])

    def test_status(self):
        condition_triggered = False
        if NodeStatus.Running:
            condition_triggered = True
        self.assertTrue(condition_triggered)

        condition_triggered = False
        if NodeStatus.Stopped:
            condition_triggered = True
        self.assertFalse(condition_triggered)

        condition_triggered = False
        if NodeStatus.Uninitialized:
            condition_triggered = True
        self.assertFalse(condition_triggered)

        with get_new_node('test') as node:
            self.assertEqual(node.get_pid(), 0)
            self.assertEqual(node.status(), NodeStatus.Uninitialized)

            node.init()

            self.assertEqual(node.get_pid(), 0)
            self.assertEqual(node.status(), NodeStatus.Stopped)

            node.start()

            self.assertTrue(node.get_pid() > 0)
            self.assertEqual(node.status(), NodeStatus.Running)

            node.stop()

            self.assertEqual(node.get_pid(), 0)
            self.assertEqual(node.status(), NodeStatus.Stopped)

            node.cleanup()

            self.assertEqual(node.get_pid(), 0)
            self.assertEqual(node.status(), NodeStatus.Uninitialized)

    def test_simple_queries(self):
        with get_new_node('test') as node:
            node.init().start()

            res = node.psql('postgres', 'select 1')
            self.assertEqual(res, (0, b'1\n', b''))

            res = node.safe_psql('postgres', 'select 1')
            self.assertEqual(res, b'1\n')

            res = node.execute('postgres', 'select 1')
            self.assertListEqual(res, [(1, )])

            with node.connect('postgres') as con:
                res = con.execute('select 1')
                self.assertListEqual(res, [(1, )])

    def test_transactions(self):
        with get_new_node('test') as node:
            node.init().start()

            with node.connect('postgres') as con:
                con.begin()
                con.execute('create table test(val int)')
                con.execute('insert into test values (1)')
                con.commit()

                con.begin()
                con.execute('insert into test values (2)')
                res = con.execute('select * from test order by val asc')
                self.assertListEqual(res, [(1, ), (2, )])
                con.rollback()

                con.begin()
                res = con.execute('select * from test')
                self.assertListEqual(res, [(1, )])
                con.rollback()

                con.begin()
                con.execute('drop table test')
                con.commit()

    def test_control_data(self):
        with get_new_node('test') as node:
            got_exception = False

            try:
                node.get_control_data()
            except ExecUtilException as e:
                got_exception = True
            except Exception as e:
                pass
            self.assertTrue(got_exception)

            got_exception = False

            try:
                node.init()
                data = node.get_control_data()
                self.assertIsNotNode(data)
            except ExecUtilException as e:
                print(e.message)
                got_exception = True
            except Exception as e:
                pass
            self.assertFalse(got_exception)

    def test_backup_simple(self):
        with get_new_node('master') as master:
            master.init(allow_streaming=True).start()

            master.psql('postgres',
                        'create table test as select generate_series(1, 4) i')

            with master.backup(xlog_method='stream') as backup:
                with backup.spawn_primary('slave') as slave:
                    slave.start()
                    res = slave.execute('postgres',
                                        'select * from test order by i asc')
                    self.assertListEqual(res, [(1, ), (2, ), (3, ), (4, )])

    def test_backup_multiple(self):
        with get_new_node('node') as node:
            node.init(allow_streaming=True).start()

            with node.backup(xlog_method='fetch') as backup1, \
                    node.backup(xlog_method='fetch') as backup2:

                self.assertNotEqual(backup1.base_dir, backup2.base_dir)

            with node.backup(xlog_method='fetch') as backup:
                with backup.spawn_primary('node1', destroy=False) as node1, \
                        backup.spawn_primary('node2', destroy=False) as node2:

                    self.assertNotEqual(node1.base_dir, node2.base_dir)

    def test_backup_exhaust(self):
        with get_new_node('node') as node:
            node.init(allow_streaming=True).start()

            with node.backup(xlog_method='fetch') as backup:
                with backup.spawn_primary('node1') as node1:
                    pass

                got_exception = False
                try:
                    with backup.spawn_primary('node2') as node2:
                        pass
                except BackupException as e:
                    got_exception = True
                except Exception as e:
                    pass

                self.assertTrue(got_exception)

    def test_backup_and_replication(self):
        with get_new_node('node') as node, get_new_node('repl') as replica:
            node.init(allow_streaming=True)
            node.start()
            node.psql('postgres', 'create table abc(a int, b int)')
            node.psql('postgres', 'insert into abc values (1, 2)')

            backup = node.backup()

            with backup.spawn_replica('replica') as replica:
                replica.start()
                res = replica.execute('postgres', 'select * from abc')
                self.assertListEqual(res, [(1, 2)])

                cur_ver = LooseVersion(get_pg_config()['VERSION_NUM'])
                min_ver = LooseVersion('10')

                # Prepare the query which would check whether record reached replica
                # (It is slightly different for Postgres 9.6 and Postgres 10+)
                if cur_ver >= min_ver:
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
                self.assertListEqual(res, [(1, 2), (3, 4)])

    def test_replicate(self):
        with get_new_node('node') as node:
            node.init(allow_streaming=True).start()

            with node.replicate(name='replica') as replica:
                res = replica.start().execute('postgres', 'select 1')
                self.assertListEqual(res, [(1, )])

    def test_dump(self):
        with get_new_node('node1') as node1:
            node1.init().start()

            with node1.connect('postgres') as con:
                con.begin()
                con.execute('create table test (val int)')
                con.execute('insert into test values (1), (2)')
                con.commit()

            # take a new dump
            dump = node1.dump('postgres')

            with get_new_node('node2') as node2:
                node2.init().start().restore('postgres', dump)

                res = node2.execute('postgres',
                                    'select * from test order by val asc')
                self.assertListEqual(res, [(1, ), (2, )])

    def test_users(self):
        with get_new_node('master') as node:
            node.init().start()
            node.psql('postgres', 'create role test_user login')
            value = node.safe_psql('postgres', 'select 1', username='test_user')
            self.assertEqual(value, six.b('1\n'))

    def test_poll_query_until(self):
        with get_new_node('master') as node:
            node.init().start()

            get_time = 'select extract(epoch from now())'
            check_time = 'select extract(epoch from now()) - {} >= 5'

            start_time = node.execute('postgres', get_time)[0][0]
            node.poll_query_until('postgres', check_time.format(start_time))
            end_time = node.execute('postgres', get_time)[0][0]

            self.assertTrue(end_time - start_time >= 5)

            # check 0 rows
            got_exception = False
            try:
                node.poll_query_until(
                    'postgres', 'select * from pg_class where true = false')
            except QueryException as e:
                got_exception = True
            self.assertTrue(got_exception)

            # check 0 columns
            got_exception = False
            try:
                node.poll_query_until('postgres',
                                      'select from pg_class limit 1')
            except QueryException as e:
                got_exception = True
            self.assertTrue(got_exception)

            # check None
            got_exception = False
            try:
                node.poll_query_until('postgres', 'create table abc (val int)')
            except QueryException as e:
                got_exception = True
            self.assertTrue(got_exception)

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

        with get_new_node('master', use_logging=True) as node0, \
            get_new_node('slave1', use_logging=True) as node1, \
                get_new_node('slave2', use_logging=True) as node2:

            node0.init().start()
            node1.init().start()
            node2.init().start()

            with open(logfile.name, 'r') as log:
                for line in log:
                    self.assertTrue(regex.match(line))

    def test_pgbench(self):
        with get_new_node('node') as node:
            node.init().start().pgbench_init()

            proc = node.pgbench(
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                options=['-T5'])

            out, _ = proc.communicate()
            out = out.decode('utf-8')

            self.assertTrue('tps' in out)

    def test_reload(self):
        with get_new_node('node') as node:
            node.init().start()

            pid1 = node.get_pid()
            node.reload()
            pid2 = node.get_pid()

            self.assertEqual(pid1, pid2)

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

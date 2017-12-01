#!/usr/bin/env python
# coding: utf-8

import os
import re
import six
import subprocess
import tempfile
import testgres
import unittest
import asynctest

import logging.config

from distutils.version import LooseVersion

from testgres import \
    InitNodeException, \
    StartNodeException, \
    ExecUtilException, \
    BackupException, \
    QueryException, \
    CatchUpException, \
    TimeoutException

from testgres import get_new_node, get_pg_config, configure_testgres
from testgres import bound_ports
from testgres import NodeStatus
from testgres import IsolationLevel


class SimpleTest(unittest.TestCase):
    def test_custom_init(self):
        with get_new_node('test') as node:
            # enable page checksums
            node.init(initdb_params=['-k']).start()
            node.safe_psql('postgres', 'select 1')

        with get_new_node('test') as node:
            node.init(allow_streaming=True,
                      initdb_params=['--auth-local=reject',
                                     '--auth-host=reject'])

            hba_file = os.path.join(node.data_dir, 'pg_hba.conf')
            with open(hba_file, 'r') as conf:
                lines = conf.readlines()

                # check number of lines
                self.assertGreaterEqual(len(lines), 6)

                # there should be no trust entries at all
                self.assertFalse(any('trust' in s for s in lines))

    def test_double_init(self):
        with get_new_node('test') as node:
            # can't initialize node more than once
            with self.assertRaises(InitNodeException):
                node.init()
                node.init()

    def test_init_after_cleanup(self):
        with get_new_node('test') as node:
            node.init().start()
            node.status()
            node.safe_psql('postgres', 'select 1')

            node.cleanup()

            node.init().start()
            node.status()
            node.safe_psql('postgres', 'select 1')

    def test_uninitialized_start(self):
        with get_new_node('test') as node:
            # node is not initialized yet
            with self.assertRaises(StartNodeException):
                node.start()

    def test_psql(self):
        with get_new_node('test') as node:
            node.init().start()

            # check default params
            with self.assertRaises(QueryException):
                node.psql('postgres')

            # check returned values
            res = node.psql('postgres', 'select 1')
            self.assertEqual(res[0], 0)
            self.assertEqual(res[1], b'1\n')
            self.assertEqual(res[2], b'')

            # check returned values
            res = node.safe_psql('postgres', 'select 1')
            self.assertEqual(res, b'1\n')

            node.stop()

            # check psql on stopped node
            with self.assertRaises(QueryException):
                node.safe_psql('postgres', 'select 1')

    def test_status(self):
        # check NodeStatus cast to bool
        condition_triggered = False
        if NodeStatus.Running:
            condition_triggered = True
        self.assertTrue(condition_triggered)

        # check NodeStatus cast to bool
        condition_triggered = False
        if NodeStatus.Stopped:
            condition_triggered = True
        self.assertFalse(condition_triggered)

        # check NodeStatus cast to bool
        condition_triggered = False
        if NodeStatus.Uninitialized:
            condition_triggered = True
        self.assertFalse(condition_triggered)

        # check statuses after each operation
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

    def test_control_data(self):
        with get_new_node('test') as node:

            # node is not initialized yet
            with self.assertRaises(ExecUtilException):
                node.get_control_data()

            node.init()
            data = node.get_control_data()

            # check returned dict
            self.assertIsNotNone(data)
            self.assertTrue(any('pg_control' in s for s in data.keys()))

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

                # exhaust backup by creating new node
                with backup.spawn_primary('node1') as node1:
                    pass

                # now let's try to create one more node
                with self.assertRaises(BackupException):
                    with backup.spawn_primary('node2') as node2:
                        pass

    def test_users(self):
        with get_new_node('master') as node:
            node.init().start()
            node.psql('postgres', 'create role test_user login')
            value = node.safe_psql('postgres', 'select 1', username='test_user')
            self.assertEqual(value, six.b('1\n'))

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

    def test_pg_ctl(self):
        with get_new_node('node') as node:
            node.init().start()

            status = node.pg_ctl(['status'])
            self.assertTrue('PID' in status)

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

        # check that port has been freed successfully
        self.assertEqual(len(bound_ports), 0)

    def test_version_management(self):
        a = LooseVersion('10.0')
        b = LooseVersion('10')
        c = LooseVersion('9.6.5')

        self.assertTrue(a > b)
        self.assertTrue(b > c)
        self.assertTrue(a > c)

    def test_configure(self):
        # set global if it wasn't set
        pg_config = get_pg_config()
        configure_testgres(cache_initdb=True, cache_pg_config=True)

        # check that is the same instance
        self.assertEqual(id(get_pg_config()), id(testgres.pg_config_data))
        configure_testgres(cache_initdb=True, cache_pg_config=False)
        self.assertNotEqual(id(get_pg_config()), id(testgres.pg_config_data))

        # return to the base state
        configure_testgres(cache_initdb=True, cache_pg_config=True)


class AsyncTest(asynctest.TestCase):
    async def test_isolation_levels(self):
        with get_new_node('node').init().start() as node:
            async with node.connect() as con:
                # string levels
                levels = [
                    'Read Uncommitted',
                    'Read Committed',
                    'Repeatable Read',
                    'Serializable',
                ]

                for l in levels:
                    await con.begin(l)
                    await con.commit()

                # enum levels
                levels = [
                    IsolationLevel.ReadUncommitted,
                    IsolationLevel.ReadCommitted,
                    IsolationLevel.RepeatableRead,
                    IsolationLevel.Serializable,
                ]

                for l in levels:
                    await con.begin(l)
                    await con.commit()

                # check wrong level
                with self.assertRaises(QueryException):
                    await con.begin('Garbage')
                    await con.commit()

    async def test_transactions(self):
        with get_new_node('test') as node:
            node.init().start()

            async with node.connect('postgres') as con:
                await con.begin()
                await con.execute('create table test(val int)')
                await con.execute('insert into test values (1)')
                await con.commit()

                await con.begin()
                await con.execute('insert into test values (2)')
                res = await con.fetch('select * from test order by val asc')
                self.assertListEqual(res, [(1, ), (2, )])
                await con.rollback()

                await con.begin()
                res = await con.fetch('select * from test')
                self.assertListEqual(res, [(1, )])
                await con.rollback()

                await con.begin()
                await con.execute('drop table test')
                await con.commit()

    async def test_simple_queries(self):
        with get_new_node('test') as node:
            node.init().start()

            res = node.psql('postgres', 'select 1')
            self.assertEqual(res, (0, b'1\n', b''))

            res = node.safe_psql('postgres', 'select 1')
            self.assertEqual(res, b'1\n')

            res = await node.fetch('postgres', 'select 1')
            self.assertListEqual(res, [(1, )])

            async with node.connect('postgres') as con:
                res = await con.fetch('select 1')
                self.assertListEqual(res, [(1, )])

    async def test_backup_and_replication(self):
        with get_new_node('node') as node:
            node.init(allow_streaming=True)
            node.start()
            node.psql('postgres', 'create table abc(a int, b int)')
            node.psql('postgres', 'insert into abc values (1, 2)')

            backup = node.backup()

            with backup.spawn_replica('replica') as replica:
                replica.start()
                res = await replica.fetch('postgres', 'select * from abc')
                self.assertListEqual(res, [(1, 2)])

                # Insert into master node
                node.psql('postgres', 'insert into abc values (3, 4)')

                # Wait until data syncronizes
                await replica.catchup()

                # Check that this record was exported to replica
                res = await replica.fetch('postgres', 'select * from abc')
                self.assertListEqual(res, [(1, 2), (3, 4)])

    async def test_backup_simple(self):
        with get_new_node('master') as master:

            # enable streaming for backups
            master.init(allow_streaming=True)

            # node must be running
            with self.assertRaises(BackupException):
                master.backup()

            # it's time to start node
            master.start()

            # fill node with some data
            master.psql('postgres',
                        'create table test as select generate_series(1, 4) i')

            with master.backup(xlog_method='stream') as backup:
                with backup.spawn_primary('slave') as slave:
                    slave.start()
                    res = await slave.fetch('postgres',
                                            'select * from test order by i asc')
                    self.assertListEqual(res, [(1, ), (2, ), (3, ), (4, )])

    async def test_dump(self):
        with get_new_node('node1') as node1:
            node1.init().start()

            async with node1.connect('postgres') as con:
                await con.begin()
                await con.execute('create table test (val int)')
                await con.execute('insert into test values (1), (2)')
                await con.commit()

            # take a new dump
            dump = node1.dump('postgres')
            self.assertTrue(os.path.isfile(dump))

            with get_new_node('node2') as node2:
                node2.init().start().restore('postgres', dump)

                res = await node2.fetch('postgres',
                                        'select * from test order by val asc')
                self.assertListEqual(res, [(1, ), (2, )])

            # finally, remove dump
            os.remove(dump)

    async def test_logging(self):
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

        with get_new_node('master', use_logging=True) as master:
            master.init().start()

            # execute a dummy query a few times
            for i in range(20):
                await master.execute('postgres', 'select 1')

            # let logging worker do the job
            import time
            time.sleep(0.5)

            # check that master's port is found
            with open(logfile.name, 'r') as log:
                lines = log.readlines()
                self.assertTrue(any('select' in s for s in lines))

    async def test_poll_query_until(self):
        with get_new_node('master') as node:
            node.init().start()

            get_time = 'select extract(epoch from now())'
            check_time = 'select extract(epoch from now()) - {} >= 5'

            start_time = (await node.fetch('postgres', get_time))[0][0]
            await node.poll_query_until('postgres', check_time.format(start_time))
            end_time = (await node.fetch('postgres', get_time))[0][0]

            self.assertTrue(end_time - start_time >= 5)

            # check 0 rows
            with self.assertRaises(QueryException):
                await node.poll_query_until(
                    'postgres', 'select * from pg_class where true = false')

            # check 0 columns
            with self.assertRaises(QueryException):
                await node.poll_query_until('postgres',
                                            'select from pg_class limit 1')
            # check None
            with self.assertRaises(QueryException):
                await node.poll_query_until('postgres', 'create table abc (val int)')

            # check timeout
            with self.assertRaises(TimeoutException):
                await node.poll_query_until(dbname='postgres',
                                            query='select 1 > 2',
                                            max_attempts=5,
                                            sleep_time=0.2)

    async def test_replicate(self):
        with get_new_node('node') as node:
            node.init(allow_streaming=True).start()

            with node.replicate(name='replica') as replica:
                res = await replica.start().fetch('postgres', 'select 1')
                self.assertListEqual(res, [(1, )])

                await node.execute(
                    'postgres', 'create table test (val int)', commit=True)

                await replica.catchup()

                res = await node.fetch('postgres', 'select * from test')
                self.assertListEqual(res, [])

    async def test_restart(self):
        with get_new_node('test') as node:
            node.init().start()
            res = await node.fetch('postgres', 'select 1')
            self.assertEqual(res, [(1, )])
            node.restart()
            res = await node.fetch('postgres', 'select 2')
            self.assertEqual(res, [(2, )])

    async def test_incorrect_catchup(self):
        with get_new_node('node') as node:
            node.init(allow_streaming=True).start()

            # node has no master, can't catch up
            with self.assertRaises(CatchUpException):
                await node.catchup()


if __name__ == '__main__':
    asynctest.main()

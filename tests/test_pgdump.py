#!/usr/bin/env python                                                           
# coding: utf-8                                                                 
                                                                                                                     
import os                                                                                                            
import subprocess                                                                                                    
import tempfile                                                                                                      
import testgres                                                                                                      
import time                                                                                                          
import unittest

import pdb                                                                                                      
                                                                                                                     
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
                                                                                                                     
from testgres import \
    TestgresConfig, \
    configure_testgres                                                                                               
                                                                                                                     
from testgres import \
    NodeStatus, \
    IsolationLevel, \
    get_new_node                                                                                                     
                                                                                                                     
from testgres import \
    get_bin_path, \
    get_pg_config                  

from testgres import bound_ports

# FIRST test from past
# with testgres.get_new_node('test') as node:
#     node.init()  # run initdb
# 	node.start() # start PostgreSQL
# 	#print(node.execute('postgres', 'select 1'))
# 	#print(node.psql('postgres', 'select 1'))
# 	print(node.connect('postgres', 'vis'))
# 	with node.connect() as con:
# 		con.begin('serializable')
# 		print(con.execute('select %s', 1))
# 		con.rollback()
# 	node.stop()  # stop PostgreSQL


# test replication behavoiur on HOT_STANDBY_FEEDBACK
# def set_trace(con, command="pg_debug"):
#     pid = con.execute("select pg_backend_pid()")[0][0]
#     p = subprocess.Popen([command], stdin=subprocess.PIPE)
#     p.communicate(str(pid).encode())

# with get_new_node() as master:
#     master.init().start()
#     with master.backup() as backup:
#         with backup.spawn_replica() as replica:
#             replica = replica.start()
#             master.execute('postgres', 'create table test (val int4)')
#             master.execute('postgres', 'insert into test values (0), (1), (2)')
#             replica.catchup()  # wait until changes are visible
#             with replica.connect() as con1:
#             	set_trace(con1)
#             	import pdb; pdb.set_trace() # Важно,если последний идет pdb,то pass
#             	pass

            # print(replica.execute('postgres', 'select count(*) from test'))
            #print(replica.execute('postgres', ':gdb'))



# SECOND test dump new keys
with get_new_node('node1') as node1:
    node1.init().start()

    with node1.connect('postgres') as con:
        con.begin()
        con.execute('create table test (val int)')
        con.execute('insert into test values (1), (2)')
        con.commit()

    # take a new dump plain format
    dump = node1.dump('postgres')
    # self.assertTrue(os.path.isfile(dump))
    with get_new_node('node2') as node2:
        node2.init().start().restore('postgres', dump)
        res = node2.execute('postgres','select * from test order by val asc')
        # self.assertListEqual(res, [(1, ), (2, )])
    # finally, remove dump
    os.remove(dump)

    # take a new dump custom format
    dump = node1.dump('postgres')
    with get_new_node('node2') as node2:
        node2.init().start().restore('postgres', dump)
        res = node2.execute('postgres','select * from test order by val asc')
    os.remove(dump)

    # take a new dump directory format
    dump = node1.dump('postgres')
    with get_new_node('node2') as node2:
        node2.init().start().restore('postgres', dump)
        res = node2.execute('postgres','select * from test order by val asc')
    os.remove(dump)

    # take a new dump tar format
    dump = node1.dump('postgres')
    with get_new_node('node2') as node2:
        node2.init().start().restore('postgres', dump)
        res = node2.execute('postgres','select * from test order by val asc')
    os.remove(dump)

    # take a new dump tar format
    dump = node1.dump('postgres')
    with get_new_node('node2') as node2:
        node2.init().start().restore('postgres', dump)
        res = node2.execute('postgres','select * from test order by val asc')
    os.remove(dump)

# 1) make dump to new place
#     pg_dump mydb > db.sql 
# 2) make dump to non default formate 
#     2.5) Чтобы сформировать выгрузку в формате plain:
#     pg_dump -Fp mydb > db.dump
#     2.1) Чтобы сформировать выгрузку в формате custom:
#     pg_dump -Fc mydb > db.dump
#     2.2) Чтобы сформировать выгрузку в формате directory:
#     pg_dump -Fd mydb -f dumpdir
#     2.3) ? Чтобы сформировать выгрузку в формате directory в 5 параллельных потоков:
#     pg_dump -Fd mydb -j 5 -f dumpdir
#     2.4) Чтобы сформировать выгрузку в формате tar:
#     pg_dump -Ft mydb > db.dump


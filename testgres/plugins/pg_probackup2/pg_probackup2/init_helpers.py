from __future__ import annotations

import logging
from functools import reduce
import getpass
import os
import re
import shutil
import subprocess
import sys
import testgres

try:
    import lz4.frame  # noqa: F401

    HAVE_LZ4 = True
except ImportError as e:
    HAVE_LZ4 = False
    LZ4_error = e

try:
    import zstd  # noqa: F401

    HAVE_ZSTD = True
except ImportError as e:
    HAVE_ZSTD = False
    ZSTD_error = e

delete_logs = os.getenv('KEEP_LOGS') not in ['1', 'y', 'Y']

try:
    testgres.configure_testgres(
        cache_initdb=False,
        cached_initdb_dir=False,
        node_cleanup_full=delete_logs)
except Exception as e:
    logging.warning("Can't configure testgres: {0}".format(e))


class Init(object):
    def __init__(self):
        pass

    @staticmethod
    def create() -> Init:
        SELF = __class__()

        if '-v' in sys.argv or '--verbose' in sys.argv:
            SELF.verbose = True
        else:
            SELF.verbose = False

        SELF._pg_config = testgres.get_pg_config()
        SELF.is_enterprise = SELF._pg_config.get('PGPRO_EDITION', None) == 'enterprise'
        SELF.is_shardman = SELF._pg_config.get('PGPRO_EDITION', None) == 'shardman'
        SELF.is_pgpro = 'PGPRO_EDITION' in SELF._pg_config
        SELF.is_nls_enabled = 'enable-nls' in SELF._pg_config['CONFIGURE']
        SELF.is_lz4_enabled = '-llz4' in SELF._pg_config['LIBS']
        version = SELF._pg_config['VERSION'].rstrip('develalphabetapre')
        parts = [*version.split(' ')[1].split('.'), '0', '0'][:3]
        parts[0] = re.match(r'\d+', parts[0]).group()
        SELF.pg_config_version = reduce(lambda v, x: v * 100 + int(x), parts, 0)

        os.environ['LANGUAGE'] = 'en'   # set default locale language to en. All messages will use this locale
        test_env = os.environ.copy()
        envs_list = [
            'LANGUAGE',
            'LC_ALL',
            'PGCONNECT_TIMEOUT',
            'PGDATA',
            'PGDATABASE',
            'PGHOSTADDR',
            'PGREQUIRESSL',
            'PGSERVICE',
            'PGSSLMODE',
            'PGUSER',
            'PGPORT',
            'PGHOST'
        ]

        for e in envs_list:
            test_env.pop(e, None)

        test_env['LC_MESSAGES'] = 'C'
        test_env['LC_TIME'] = 'C'
        SELF._test_env = test_env

        # Get the directory from which the script was executed
        SELF.source_path = os.getcwd()
        tmp_path = test_env.get('PGPROBACKUP_TMP_DIR')
        if tmp_path and os.path.isabs(tmp_path):
            SELF.tmp_path = tmp_path
        else:
            SELF.tmp_path = os.path.abspath(
                os.path.join(SELF.source_path, tmp_path or os.path.join('tests', 'tmp_dirs'))
            )

        os.makedirs(SELF.tmp_path, exist_ok=True)

        SELF.username = getpass.getuser()

        SELF.probackup_path = None
        if 'PGPROBACKUPBIN' in test_env:
            if shutil.which(test_env["PGPROBACKUPBIN"]):
                SELF.probackup_path = test_env["PGPROBACKUPBIN"]
            else:
                if SELF.verbose:
                    print('PGPROBACKUPBIN is not an executable file')

        if not SELF.probackup_path:
            probackup_path_tmp = os.path.join(
                testgres.get_pg_config()['BINDIR'], 'pg_probackup')

            if os.path.isfile(probackup_path_tmp):
                if not os.access(probackup_path_tmp, os.X_OK):
                    logging.warning('{0} is not an executable file'.format(
                        probackup_path_tmp))
                else:
                    SELF.probackup_path = probackup_path_tmp

        if not SELF.probackup_path:
            probackup_path_tmp = SELF.source_path

            if os.path.isfile(probackup_path_tmp):
                if not os.access(probackup_path_tmp, os.X_OK):
                    logging.warning('{0} is not an executable file'.format(
                        probackup_path_tmp))
                else:
                    SELF.probackup_path = probackup_path_tmp

        if not SELF.probackup_path:
            logging.error('pg_probackup binary is not found')
            return None

        if os.name == 'posix':
            SELF.EXTERNAL_DIRECTORY_DELIMITER = ':'
            os.environ['PATH'] = os.path.dirname(
                SELF.probackup_path) + ':' + os.environ['PATH']

        elif os.name == 'nt':
            SELF.EXTERNAL_DIRECTORY_DELIMITER = ';'
            os.environ['PATH'] = os.path.dirname(
                SELF.probackup_path) + ';' + os.environ['PATH']

        SELF.probackup_old_path = None
        if 'PGPROBACKUPBIN_OLD' in test_env:
            if (os.path.isfile(test_env['PGPROBACKUPBIN_OLD']) and os.access(test_env['PGPROBACKUPBIN_OLD'], os.X_OK)):
                SELF.probackup_old_path = test_env['PGPROBACKUPBIN_OLD']
            else:
                if SELF.verbose:
                    print('PGPROBACKUPBIN_OLD is not an executable file')

        SELF.probackup_version = None
        SELF.old_probackup_version = None

        probackup_version_output = subprocess.check_output(
            [SELF.probackup_path, "--version"],
            stderr=subprocess.STDOUT,
        ).decode('utf-8')
        match = re.search(r"\d+\.\d+\.\d+",
                          probackup_version_output)
        SELF.probackup_version = match.group(0) if match else None
        match = re.search(r"\(compressions: ([^)]*)\)", probackup_version_output)
        compressions = match.group(1) if match else None
        if compressions:
            SELF.probackup_compressions = {s.strip() for s in compressions.split(',')}
        else:
            SELF.probackup_compressions = []

        if SELF.probackup_old_path:
            old_probackup_version_output = subprocess.check_output(
                [SELF.probackup_old_path, "--version"],
                stderr=subprocess.STDOUT,
            ).decode('utf-8')
            match = re.search(r"\d+\.\d+\.\d+",
                              old_probackup_version_output)
            SELF.old_probackup_version = match.group(0) if match else None

        SELF.remote = test_env.get('PGPROBACKUP_SSH_REMOTE', None) == 'ON'
        SELF.ptrack = test_env.get('PG_PROBACKUP_PTRACK', None) == 'ON' and SELF.pg_config_version >= 110000
        SELF.wal_tree_enabled = test_env.get('PG_PROBACKUP_WAL_TREE_ENABLED', None) == 'ON'

        SELF.bckp_source = test_env.get('PG_PROBACKUP_SOURCE', 'pro').lower()
        if SELF.bckp_source not in ('base', 'direct', 'pro'):
            raise Exception("Wrong PG_PROBACKUP_SOURCE value. Available options: base|direct|pro")

        SELF.paranoia = test_env.get('PG_PROBACKUP_PARANOIA', None) == 'ON'
        env_compress = test_env.get('ARCHIVE_COMPRESSION', None)
        if env_compress:
            env_compress = env_compress.lower()
        if env_compress in ('on', 'zlib'):
            SELF.compress_suffix = '.gz'
            SELF.archive_compress = 'zlib'
        elif env_compress == 'lz4':
            if not HAVE_LZ4:
                raise LZ4_error
            if 'lz4' not in SELF.probackup_compressions:
                raise Exception("pg_probackup is not compiled with lz4 support")
            SELF.compress_suffix = '.lz4'
            SELF.archive_compress = 'lz4'
        elif env_compress == 'zstd':
            if not HAVE_ZSTD:
                raise ZSTD_error
            if 'zstd' not in SELF.probackup_compressions:
                raise Exception("pg_probackup is not compiled with zstd support")
            SELF.compress_suffix = '.zst'
            SELF.archive_compress = 'zstd'
        else:
            SELF.compress_suffix = ''
            SELF.archive_compress = False

        cfs_compress = test_env.get('PG_PROBACKUP_CFS_COMPRESS', None)
        if cfs_compress:
            SELF.cfs_compress = cfs_compress.lower()
        else:
            SELF.cfs_compress = SELF.archive_compress

        os.environ["PGAPPNAME"] = "pg_probackup"
        SELF.delete_logs = delete_logs

        if SELF.probackup_version.split('.')[0].isdigit():
            SELF.major_version = int(SELF.probackup_version.split('.')[0])
        else:
            logging.error('Can\'t process pg_probackup version \"{}\": the major version is expected to be a number'.format(SELF.probackup_version))
            return None

        return SELF

    def test_env(self):
        return self._test_env.copy()


init_params = Init.create()

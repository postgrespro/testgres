# coding: utf-8

# names for dirs in base_dir
DATA_DIR = "data"
LOGS_DIR = "logs"

# prefixes for temp dirs
TMP_NODE = 'tgsn_'
TMP_DUMP = 'tgsd_'
TMP_CACHE = 'tgsc_'
TMP_BACKUP = 'tgsb_'

TMP_TESTGRES = "testgres"

TMP_TESTGRES_PORTS = TMP_TESTGRES + "/ports"

# path to control file
XLOG_CONTROL_FILE = "global/pg_control"

# names for config files
RECOVERY_CONF_FILE = "recovery.conf"
PG_AUTO_CONF_FILE = "postgresql.auto.conf"
PG_CONF_FILE = "postgresql.conf"
PG_PID_FILE = 'postmaster.pid'
HBA_CONF_FILE = "pg_hba.conf"

# names for log files
PG_LOG_FILE = "postgresql.log"
UTILS_LOG_FILE = "utils.log"
BACKUP_LOG_FILE = "backup.log"

# defaults for node settings
MAX_LOGICAL_REPLICATION_WORKERS = 5
MAX_REPLICATION_SLOTS = 10
MAX_WORKER_PROCESSES = 10
WAL_KEEP_SEGMENTS = 20
WAL_KEEP_SIZE = 320
MAX_WAL_SENDERS = 10

# logical replication settings
LOGICAL_REPL_MAX_CATCHUP_ATTEMPTS = 60

PG_CTL__STATUS__OK = 0
PG_CTL__STATUS__NODE_IS_STOPPED = 3
PG_CTL__STATUS__BAD_DATADIR = 4

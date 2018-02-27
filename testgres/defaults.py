import datetime
import getpass
import os
import struct
import uuid


def default_dbname():
    """
    Return default DB name.
    """

    return 'postgres'


def default_username():
    """
    Return default username (current user).
    """

    return getpass.getuser()


def generate_app_name():
    """
    Generate a new application name for node.
    """

    return 'testgres-{}'.format(str(uuid.uuid4()))


def generate_system_id():
    """
    Generate a new 64-bit unique system identifier for node.
    """

    date1 = datetime.datetime.utcfromtimestamp(0)
    date2 = datetime.datetime.utcnow()

    secs = int((date2 - date1).total_seconds())
    usecs = date2.microsecond

    # see pg_resetwal.c : GuessControlValues()
    system_id = 0
    system_id |= (secs << 32)
    system_id |= (usecs << 12)
    system_id |= (os.getpid() & 0xFFF)

    # pack ULL in native byte order
    return struct.pack('=Q', system_id)

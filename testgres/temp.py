import atexit
import os
import tempfile

from shutil import rmtree

from .consts import TMP_DEFAULT

temp_files = set()


def set_temp_root(path):
    """
    Set a root dir for temporary files.
    """

    tempfile.tempdir = path


def get_temp_root():
    """
    Get current root dir for temporary files.
    """

    return tempfile.tempdir


def forget_temp_obj(path):
    """
    Do not remove a file or dir if it's provided by this module.
    """

    temp_files.discard(path)


def mk_temp_file(prefix=TMP_DEFAULT, delete=True):
    """
    Provide a temporary file.
    """

    fd, fname = tempfile.mkstemp(prefix=prefix)
    os.close(fd)
    if delete:
        temp_files.add(fname)
    return fname


def mk_temp_dir(prefix=TMP_DEFAULT, delete=True):
    """
    Provide a temporary directory.
    """

    dname = tempfile.mkdtemp(prefix=prefix)
    if delete:
        temp_files.add(dname)
    return dname


@atexit.register
def _rm_temp():
    for f in temp_files:
        if not os.path.exists(f):
            continue

        if os.path.isdir(f):
            rmtree(f, ignore_errors=True)
        if os.path.isfile(f):
            os.remove(f)

"""Linux fault-injection lab built on top of Testgres.

PgLab keeps PostgreSQL lifecycle, backup and replication in Testgres while
placing individual nodes into separate Linux network namespaces and cgroups.
It is Linux-only and currently requires root privileges.
"""

from __future__ import annotations

import hashlib
import ipaddress
import os
import pwd
import signal
import shutil
import stat
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .api import get_new_node
from testgres.operations.local_ops import LocalOperations


class PgLabError(RuntimeError):
    pass


def _run(cmd, check=True, capture=False):
    kwargs = {"check": check, "text": True}
    if capture:
        kwargs.update(stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return subprocess.run(list(cmd), **kwargs)


def _which(name):
    path = shutil.which(name)
    if not path:
        raise PgLabError("required command not found: {}".format(name))
    return path


def _ifname(prefix, name):
    # Linux IFNAMSIZ is 16 including the terminating NUL.
    digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:6]
    return (prefix + digest)[:15]


@dataclass
class IOLimits:
    read_bps: Optional[int] = None
    write_bps: Optional[int] = None
    read_iops: Optional[int] = None
    write_iops: Optional[int] = None

    def as_io_max(self, major, minor):
        parts = []
        if self.read_bps is not None:
            parts.append("rbps={}".format(self.read_bps))
        if self.write_bps is not None:
            parts.append("wbps={}".format(self.write_bps))
        if self.read_iops is not None:
            parts.append("riops={}".format(self.read_iops))
        if self.write_iops is not None:
            parts.append("wiops={}".format(self.write_iops))
        if not parts:
            return None
        return "{}:{} {}".format(major, minor, " ".join(parts))


class CgroupIOBackend:
    version = None

    def setup_root(self):
        raise NotImplementedError

    def create_node(self, name):
        raise NotImplementedError

    def apply_limits(self, cgroup, device, limits):
        raise NotImplementedError

    def membership_file(self, cgroup):
        raise NotImplementedError

    def kill_node(self, cgroup):
        pass

    def remove_node(self, cgroup):
        cgroup.rmdir()

    def cleanup_root(self):
        try:
            self.root.rmdir()
        except OSError:
            pass


class CgroupV2IO(CgroupIOBackend):
    version = 2

    def __init__(self, root=Path("/sys/fs/cgroup/testgres-pglab")):
        self.mount = Path("/sys/fs/cgroup")
        self.root = Path(root)

    def setup_root(self):
        controllers = (self.mount / "cgroup.controllers").read_text().split()
        if "io" not in controllers:
            raise PgLabError("cgroup v2 io controller is unavailable")
        subtree = self.mount / "cgroup.subtree_control"
        enabled = subtree.read_text().split()
        if "io" not in enabled:
            try:
                subtree.write_text("+io\n")
            except OSError as exc:
                raise PgLabError("cannot enable cgroup v2 io controller: {}".format(exc))
        self.root.mkdir(exist_ok=True)
        child_subtree = self.root / "cgroup.subtree_control"
        available = (self.root / "cgroup.controllers").read_text().split()
        if "io" in available and "io" not in child_subtree.read_text().split():
            child_subtree.write_text("+io\n")

    def create_node(self, name):
        path = self.root / name
        path.mkdir(exist_ok=True)
        return path

    def apply_limits(self, cgroup, device, limits):
        st = os.stat(device)
        line = limits.as_io_max(os.major(st.st_rdev), os.minor(st.st_rdev))
        if line:
            (cgroup / "io.max").write_text(line + "\n")

    def membership_file(self, cgroup):
        return cgroup / "cgroup.procs"

    def kill_node(self, cgroup):
        try:
            (cgroup / "cgroup.kill").write_text("1\n")
        except OSError:
            pass


class CgroupV1Blkio(CgroupIOBackend):
    version = 1

    def __init__(self, mount=Path("/sys/fs/cgroup/blkio")):
        self.mount = Path(mount)
        self.root = self.mount / "testgres-pglab"

    def setup_root(self):
        if not self.mount.is_dir():
            raise PgLabError("cgroup v1 blkio controller is unavailable")
        self.root.mkdir(exist_ok=True)

    def create_node(self, name):
        path = self.root / name
        path.mkdir(exist_ok=True)
        return path

    def _write_limit(self, cgroup, filename, major, minor, value):
        if value is not None:
            (cgroup / filename).write_text("{}:{} {}\n".format(major, minor, value))

    def apply_limits(self, cgroup, device, limits):
        st = os.stat(device)
        major, minor = os.major(st.st_rdev), os.minor(st.st_rdev)
        self._write_limit(cgroup, "blkio.throttle.read_bps_device", major, minor, limits.read_bps)
        self._write_limit(cgroup, "blkio.throttle.write_bps_device", major, minor, limits.write_bps)
        self._write_limit(cgroup, "blkio.throttle.read_iops_device", major, minor, limits.read_iops)
        self._write_limit(cgroup, "blkio.throttle.write_iops_device", major, minor, limits.write_iops)

    def membership_file(self, cgroup):
        return cgroup / "tasks"


def detect_cgroup_io_backend():
    # Pure/unified v2 has cgroup.controllers at the top-level mount.
    if Path("/sys/fs/cgroup/cgroup.controllers").is_file():
        return CgroupV2IO()
    # Ubuntu 20.04 commonly uses hybrid mode: cgroup2 is mounted under
    # /sys/fs/cgroup/unified, while blkio remains on the v1 hierarchy.
    if Path("/sys/fs/cgroup/blkio").is_dir():
        return CgroupV1Blkio()
    raise PgLabError("no supported cgroup I/O controller found (need v2 io or v1 blkio)")


class NamespaceOperations(LocalOperations):
    """Local Testgres operations whose *commands* execute in a netns.

    Filesystem operations intentionally remain ordinary local operations: a Linux
    network namespace shares the host filesystem.  PostgreSQL utilities launched
    by Testgres (initdb, pg_ctl, psql, pg_basebackup, pg_isready, ...) are wrapped
    in::

        ip netns exec <namespace> ... runuser -u <os_user> -- <command>

    The wrapper enters the node cgroup before dropping privileges, so postgres
    and all of its descendants inherit the cgroup.
    """

    def __init__(self, namespace, os_user, db_user, cgroup=None, cgroup_membership_file=None, pg_bin=None):
        super().__init__()
        self.namespace = namespace
        self.os_user = os_user
        self.db_user = db_user
        self.pg_bin = str(pg_bin) if pg_bin is not None else None
        self.cgroup = Path(cgroup) if cgroup is not None else None
        self.cgroup_membership_file = (
            Path(cgroup_membership_file) if cgroup_membership_file is not None else None
        )

        # Testgres uses os_ops.username as the default database username in a
        # number of places.  Keep it distinct from the Unix account we use in
        # runuser.
        self._username = db_user

    def environ(self, name):
        # target_os_ops may be asked to resolve PostgreSQL binaries while a
        # replica is being cloned.  Expose the PgLab-specific bin directory to
        # Testgres without requiring the caller to mutate the process-wide
        # PG_BIN environment variable.
        if name == "PG_BIN" and self.pg_bin is not None:
            return self.pg_bin
        return super().environ(name)

    def mkdtemp(self, prefix=None):
        assert prefix is None or type(prefix) is str

        base = "/tmp/testgres-pglab-tmp"
        os.makedirs(base, mode=0o755, exist_ok=True)

        path = tempfile.mkdtemp(
            prefix=prefix,
            dir=base,
        )

        pw = pwd.getpwnam(self.os_user)
        os.chown(path, pw.pw_uid, pw.pw_gid)
        os.chmod(path, 0o700)

        return path

    def mkstemp(self, prefix=None):
        assert prefix is None or type(prefix) is str

        base = "/tmp/testgres-pglab-tmp"
        os.makedirs(base, mode=0o755, exist_ok=True)

        fd, path = tempfile.mkstemp(
            prefix=prefix,
            dir=base,
        )
        os.close(fd)

        pw = pwd.getpwnam(self.os_user)
        os.chown(path, pw.pw_uid, pw.pw_gid)
        os.chmod(path, 0o600)

        return path

    def create_clone(self):
        return self.__class__(
            namespace=self.namespace,
            os_user=self.os_user,
            db_user=self.db_user,
            cgroup=str(self.cgroup) if self.cgroup is not None else None,
            cgroup_membership_file=(
                str(self.cgroup_membership_file)
                if self.cgroup_membership_file is not None else None
            ),
            pg_bin=self.pg_bin,
        )

    def _command_needs_server_cgroup(self, payload):
        """Return True only for commands that may start the postgres server.

        Client utilities such as psql, pg_basebackup and pg_isready should not
        be charged to the server's I/O cgroup.  pg_ctl inherits the cgroup and
        the postmaster it starts keeps that membership.
        """
        if not payload:
            return False
        exe = os.path.basename(str(payload[0]))
        if exe == "postgres":
            return True
        if exe == "pg_ctl":
            # Testgres normally invokes pg_ctl with an explicit action.  Only
            # start/restart can create a postmaster that needs to inherit the
            # node cgroup.
            args = [str(x) for x in payload[1:]]
            return "start" in args or "restart" in args
        return False

    def _with_pg_path(self, argv):
        if self.pg_bin is None:
            return list(argv)
        path = self.pg_bin + os.pathsep + os.environ.get("PATH", "")
        return ["env", "PATH=" + path] + list(argv)

    def _wrap_command(self, cmd, shell):
        if shell:
            if isinstance(cmd, str):
                command = cmd
            else:
                command = self.join_command_arguments(list(cmd))
            payload = ["sh", "-c", command]
        else:
            if isinstance(cmd, str):
                payload = ["sh", "-c", cmd]
            else:
                payload = list(cmd)

        prefix = ["ip", "netns", "exec", self.namespace]
        run_as_user = ["runuser", "-u", self.os_user, "--"] + self._with_pg_path(payload)

        # Most Testgres utilities are clients.  They belong to the controller,
        # not to the PostgreSQL server cgroup.
        if self.cgroup is None or not self._command_needs_server_cgroup(payload):
            return prefix + run_as_user, False

        membership = self.cgroup_membership_file
        if membership is None:
            raise PgLabError("cgroup membership file was not configured")
        if not membership.exists():
            raise PgLabError(
                "node cgroup disappeared before postgres start: {}".format(membership)
            )

        # Join the cgroup in the host namespace first, then enter the network
        # namespace.  Some systems do not expose the host cgroup filesystem from
        # an ip-netns helper mount namespace, while cgroup membership itself is
        # inherited perfectly well across ip netns exec/runuser/pg_ctl.
        script = (
            'echo $$ > "$1"; '
            'ns="$2"; user="$3"; shift 3; '
            'exec ip netns exec "$ns" runuser -u "$user" -- "$@"'
        )
        wrapped = [
            "sh", "-c", script, "pglab-cgroup", str(membership),
            self.namespace, self.os_user,
        ] + self._with_pg_path(payload)
        return wrapped, False

    def exec_command(self, cmd, *args, **kwargs):
        shell = kwargs.pop("shell", False)
        wrapped, wrapped_shell = self._wrap_command(cmd, shell)
        return super().exec_command(wrapped, *args, shell=wrapped_shell, **kwargs)

    def popen(self, cmd, *args, **kwargs):
        shell = kwargs.pop("shell", False)
        wrapped, wrapped_shell = self._wrap_command(cmd, shell)
        return super().popen(wrapped, *args, shell=wrapped_shell, **kwargs)


class LabNode:
    """A Linux-isolated node backed by a real testgres.PostgresNode."""

    def __init__(self, lab, name, ip, namespace, veth_host, cgroup,
                 pg_node, os_ops, role):
        self.lab = lab
        self.name = name
        self.ip = ip
        self.namespace = namespace
        self.veth_host = veth_host
        self.veth_ns = "eth0"
        self.cgroup = cgroup
        self.pg = pg_node
        self.os_ops = os_ops
        self.role = role

    def __getattr__(self, name):
        # Keep the normal Testgres API pleasant to use:
        # node.execute(), node.safe_psql(), node.append_conf(), ...
        return getattr(self.pg, name)

    def init(self, initdb_params=None, **kwargs):
        params = list(initdb_params or [])
        if "-U" not in params and "--username" not in params:
            params += ["-U", self.lab.db_superuser]
        self.lab._ensure_node_ownership(self)
        self.pg.init(initdb_params=params, **kwargs)

        # Network namespaces isolate TCP/IP but share the host filesystem.
        # Without this, nodes using the same PostgreSQL port collide on
        # /tmp/.s.PGSQL.<port> even though their TCP sockets are isolated.
        self.pg.append_conf(unix_socket_directories="")

        self.lab._ensure_node_ownership(self)
        return self

    def start(self, *args, **kwargs):
        # Testgres may lazily create logs_dir during start().  Force creation
        # while the controller is root, then hand the whole node tree to the
        # configured OS user before pg_ctl is run through runuser.
        _ = self.pg.logs_dir
        self.lab._ensure_node_ownership(self)
        self.pg.start(*args, **kwargs)
        return self

    def stop(self, *args, **kwargs):
        self.pg.stop(*args, **kwargs)
        return self

    def network_down(self):
        self.lab.network_down(self.name)
        return self

    def network_up(self):
        self.lab.network_up(self.name)
        return self

    def netem(self, **kwargs):
        self.lab.netem(self.name, **kwargs)
        return self

    def clear_netem(self):
        self.lab.clear_netem(self.name)
        return self

    def freeze(self):
        self.lab.freeze(self.name)
        return self

    def unfreeze(self):
        self.lab.unfreeze(self.name)
        return self


class PgLab:
    """Testgres + Linux network namespaces + cgroup I/O fault injection.

    PostgreSQL lifecycle is delegated to Testgres.  PgLab owns only Linux
    isolation (netns/veth/bridge/cgroup) and fault injection.
    """

    def __init__(
        self,
        pg_bin=None,
        base_dir="/tmp/testgres-pglab",
        subnet="10.203.0.0/24",
        bridge_name="pglab0",
        bridge_ip=None,
        os_user="postgres",
        db_superuser=None,
        keep_data=False,
    ):
        self.pg_bin = str(pg_bin) if pg_bin is not None else None
        self.base_dir = Path(base_dir)
        self.subnet = ipaddress.ip_network(subnet, strict=True)
        self.bridge_name = bridge_name
        self.os_user = os_user
        self.db_superuser = db_superuser or os_user
        self.keep_data = keep_data
        self.nodes = {}  # type: Dict[str, LabNode]
        self._created_bridge = False
        self._closed = False
        self._firewall_backend = None
        self._firewall_source_added = False
        self._iptables_rule_added = False

        hosts = self.subnet.hosts()
        default_bridge_ip = str(next(hosts))
        self.bridge_ip = bridge_ip or default_bridge_ip
        if ipaddress.ip_address(self.bridge_ip) not in self.subnet:
            raise PgLabError("bridge IP is outside lab subnet: {}".format(self.bridge_ip))

        self._preflight()
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._setup_cgroups()
        self._ensure_bridge()
        self._setup_firewall()

    def _preflight(self):
        if os.name != "posix" or not Path("/proc/self/ns/net").exists():
            raise PgLabError("pglab requires Linux network namespaces")
        if os.geteuid() != 0:
            raise PgLabError("pglab must currently be run as root")
        _which("ip")
        _which("runuser")
        _which("findmnt")
        try:
            pwd.getpwnam(self.os_user)
        except KeyError as exc:
            raise PgLabError("OS user does not exist: {}".format(self.os_user)) from exc
        # cgroup mode is detected separately. Ubuntu 20.04 commonly uses
        # hybrid mode with blkio on cgroup v1.
        if self.pg_bin is not None:
            required = ("initdb", "pg_ctl", "postgres", "psql", "pg_basebackup")
            for exe in required:
                p = Path(self.pg_bin) / exe
                if not p.is_file() or not os.access(str(p), os.X_OK):
                    raise PgLabError("PostgreSQL executable missing/not executable: {}".format(p))

    def _setup_cgroups(self):
        self.cgroup_backend = detect_cgroup_io_backend()
        self.cgroup_backend.setup_root()
        self.cgroup_root = self.cgroup_backend.root

    def _ensure_bridge(self):
        exists = subprocess.run(
            ["ip", "link", "show", "dev", self.bridge_name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).returncode == 0
        if not exists:
            _run(["ip", "link", "add", self.bridge_name, "type", "bridge"])
            self._created_bridge = True

        # The host gets an address in the lab subnet.  This is important because
        # testgres' Python DB driver connects from the controller namespace to
        # node.host; unlike psql, execute()/connect() do not go through os_ops.
        addr = "{}/{}".format(self.bridge_ip, self.subnet.prefixlen)
        _run(["ip", "addr", "replace", addr, "dev", self.bridge_name])
        _run(["ip", "link", "set", self.bridge_name, "up"])

    def _setup_firewall(self):
        """Allow traffic between PgLab nodes without changing persistent rules.

        firewalld and iptables-nft can be independent forwarding layers.  When
        firewalld is active we add the lab subnet to the runtime trusted zone,
        and independently add a temporary iptables FORWARD allow rule.
        """
        source = str(self.subnet)
        firewall_cmd = shutil.which("firewall-cmd")

        if firewall_cmd is not None:
            state = subprocess.run(
                [firewall_cmd, "--state"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            if state.returncode == 0:
                zone_proc = subprocess.run(
                    [firewall_cmd, "--get-zone-of-source={}".format(source)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                zone = zone_proc.stdout.strip()
                if zone and zone != "trusted":
                    raise PgLabError(
                        "lab subnet {} already belongs to firewalld zone {!r}; "
                        "refusing to move an existing firewall configuration".format(
                            source, zone
                        )
                    )
                if zone != "trusted":
                    proc = subprocess.run(
                        [
                            firewall_cmd,
                            "--zone=trusted",
                            "--add-source={}".format(source),
                        ],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.PIPE,
                        text=True,
                    )
                    if proc.returncode != 0:
                        raise PgLabError(
                            "failed to add lab subnet {} to firewalld trusted zone: {}".format(
                                source, proc.stderr.strip()
                            )
                        )
                    self._firewall_source_added = True
                self._firewall_backend = "firewalld"

        # firewalld and iptables-nft are not alternatives on many developer
        # hosts. Docker/libvirt may leave an iptables FORWARD base chain with
        # policy DROP at nft priority 0, while firewalld evaluates forwarding
        # again in a later base chain (commonly priority +10).  Both layers
        # therefore need a temporary allow for the lab subnet.
        iptables = shutil.which("iptables")
        if iptables is None:
            return

        rule = [
            "FORWARD", "-s", source, "-d", source, "-j", "ACCEPT",
        ]
        exists = subprocess.run(
            [iptables, "-C"] + rule,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).returncode == 0
        if not exists:
            _run([iptables, "-I"] + rule)
            self._iptables_rule_added = True

        if self._firewall_backend is None:
            self._firewall_backend = "iptables"

    def _cleanup_firewall(self):
        source = str(self.subnet)

        # The two firewall layers are independent and can both be active.
        # Remove only rules that this PgLab instance actually added.
        if self._firewall_source_added:
            firewall_cmd = shutil.which("firewall-cmd")
            if firewall_cmd is not None:
                subprocess.run(
                    [
                        firewall_cmd,
                        "--zone=trusted",
                        "--remove-source={}".format(source),
                    ],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            self._firewall_source_added = False

        if self._iptables_rule_added:
            iptables = shutil.which("iptables")
            if iptables is not None:
                subprocess.run(
                    [
                        iptables, "-D", "FORWARD",
                        "-s", source, "-d", source,
                        "-j", "ACCEPT",
                    ],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            self._iptables_rule_added = False

    @staticmethod
    def _sysfs_block_path(device):
        st = os.stat(device)
        if not stat.S_ISBLK(st.st_mode):
            raise PgLabError("not a block device: {}".format(device))
        return Path("/sys/dev/block/{}:{}".format(os.major(st.st_rdev), os.minor(st.st_rdev)))

    @staticmethod
    def _device_from_sysfs(sysdev):
        # sysdev may be a /sys/dev/block/M:m symlink or a path reached through
        # a device-mapper/md 'slaves' directory.  The kernel exposes DEVNAME
        # in uevent for both disks and partitions.
        uevent = Path(sysdev) / "uevent"
        if not uevent.is_file():
            raise PgLabError("cannot resolve block device from sysfs: {}".format(sysdev))
        values = {}
        for line in uevent.read_text().splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                values[k] = v
        devname = values.get("DEVNAME")
        if not devname:
            raise PgLabError("DEVNAME is missing in {}".format(uevent))
        return "/dev/{}".format(devname)

    def _leaf_block_devices(self, device):
        """Return physical queue devices suitable for blkio throttling.

        findmnt may return a partition or a stacked device such as LVM/dm-crypt.
        cgroup-v1 blkio throttling can reject such a major:minor with ENODEV.
        Walk sysfs 'slaves' recursively and, for partitions, use the parent disk.
        """
        start = self._sysfs_block_path(device).resolve()
        found = {}

        def visit(sysdev):
            sysdev = Path(sysdev).resolve()
            slaves = sysdev / "slaves"
            if slaves.is_dir():
                children = list(slaves.iterdir())
                if children:
                    for child in children:
                        visit(child)
                    return

            # A partition has its own major:minor, but blkio v1 throttling is
            # most portable when applied to the whole queue-bearing disk.
            if (sysdev / "partition").is_file():
                parent = sysdev.parent
                if (parent / "dev").is_file():
                    sysdev = parent

            dev = self._device_from_sysfs(sysdev)
            st = os.stat(dev)
            key = (os.major(st.st_rdev), os.minor(st.st_rdev))
            found[key] = dev

        visit(start)
        return [found[k] for k in sorted(found)]

    def _backing_block_devices(self, path):
        res = _run(["findmnt", "-n", "-o", "SOURCE", "-T", str(path)], capture=True)
        source = res.stdout.strip()
        if not source.startswith("/dev/"):
            raise PgLabError(
                "cannot infer block device for {}: {!r}; pass io_device".format(path, source)
            )
        devices = self._leaf_block_devices(source)
        if not devices:
            raise PgLabError("cannot resolve backing block device for {}".format(source))
        return devices

    def _apply_io_limits(self, cgroup, devices, limits):
        if isinstance(devices, (str, Path)):
            devices = [str(devices)]
        for device in devices:
            st = os.stat(device)
            if not stat.S_ISBLK(st.st_mode):
                raise PgLabError("not a block device: {}".format(device))
            self.cgroup_backend.apply_limits(cgroup, device, limits)

    def _ensure_node_ownership(self, node):
        pw = pwd.getpwnam(self.os_user)
        base = Path(node.pg.base_dir)
        base.mkdir(parents=True, exist_ok=True)
        for root, dirs, files in os.walk(str(base)):
            os.chown(root, pw.pw_uid, pw.pw_gid)
            for d in dirs:
                os.chown(os.path.join(root, d), pw.pw_uid, pw.pw_gid)
            for f in files:
                try:
                    os.chown(os.path.join(root, f), pw.pw_uid, pw.pw_gid)
                except FileNotFoundError:
                    pass

    def create_node(self, name, ip, role="standalone", io=None, io_device=None, port=5432):
        if name in self.nodes:
            raise PgLabError("node already exists: {}".format(name))
        if role not in ("primary", "replica", "standalone"):
            raise PgLabError("invalid role: {}".format(role))

        addr_obj = ipaddress.ip_address(ip)
        if addr_obj not in self.subnet:
            raise PgLabError("{} is outside lab subnet {}".format(ip, self.subnet))
        if ip == self.bridge_ip:
            raise PgLabError("{} is reserved for the pglab bridge".format(ip))
        if any(n.ip == ip for n in self.nodes.values()):
            raise PgLabError("IP already in use: {}".format(ip))

        ns = "pglab-{}".format(name)
        vh = _ifname("ph", name)
        peer = _ifname("pn", name)
        cgroup = self.cgroup_root / name
        node_base = self.base_dir / name

        created_ns = False
        created_veth = False
        created_cgroup = False
        try:
            _run(["ip", "netns", "add", ns])
            created_ns = True
            _run(["ip", "link", "add", vh, "type", "veth", "peer", "name", peer])
            created_veth = True
            _run(["ip", "link", "set", peer, "netns", ns])
            _run(["ip", "link", "set", vh, "master", self.bridge_name])
            _run(["ip", "link", "set", vh, "up"])
            _run(["ip", "-n", ns, "link", "set", "lo", "up"])
            _run(["ip", "-n", ns, "link", "set", peer, "name", "eth0"])
            _run(["ip", "-n", ns, "addr", "add",
                  "{}/{}".format(ip, self.subnet.prefixlen), "dev", "eth0"])
            _run(["ip", "-n", ns, "link", "set", "eth0", "up"])

            node_base.mkdir(parents=True, exist_ok=True)
            pw = pwd.getpwnam(self.os_user)
            os.chown(str(node_base), pw.pw_uid, pw.pw_gid)

            cgroup = self.cgroup_backend.create_node(name)
            created_cgroup = True
            if io is not None:
                devices = [io_device] if io_device else self._backing_block_devices(node_base)
                self._apply_io_limits(cgroup, devices, io)

            os_ops = NamespaceOperations(
                namespace=ns,
                os_user=self.os_user,
                db_user=self.db_superuser,
                cgroup=cgroup,
                cgroup_membership_file=self.cgroup_backend.membership_file(cgroup),
                pg_bin=self.pg_bin,
            )

            kwargs = {
                "name": name,
                "base_dir": str(node_base),
                "port": int(port),
                "host": ip,
                "os_ops": os_ops,
            }
            if self.pg_bin is not None:
                kwargs["bin_dir"] = self.pg_bin
            pg_node = get_new_node(**kwargs)

            node = LabNode(
                lab=self,
                name=name,
                ip=ip,
                namespace=ns,
                veth_host=vh,
                cgroup=cgroup,
                pg_node=pg_node,
                os_ops=os_ops,
                role=role,
            )
            self.nodes[name] = node
            return node
        except Exception:
            if created_veth:
                subprocess.run(["ip", "link", "del", vh], stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL)
            if created_ns:
                subprocess.run(["ip", "netns", "del", ns], stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL)
            if created_cgroup:
                try:
                    cgroup.rmdir()
                except OSError:
                    pass
            if not self.keep_data:
                shutil.rmtree(str(node_base), ignore_errors=True)
            raise

    def create_primary(self, name, ip, **kwargs):
        return self.create_node(name, ip, role="primary", **kwargs)

    def create_replica(self, primary_name, name, ip, **kwargs):
        """Create a physical replica in its own network namespace.

        Current Testgres can run pg_basebackup directly in ``target_os_ops``.
        PgLab creates the namespace/cgroup first, then asks Testgres to create
        the replica through that execution context.  The base backup therefore
        originates from the replica namespace instead of being copied into it
        afterwards.
        """
        primary = self.nodes[primary_name]
        replica_shell = self.create_node(name, ip, role="replica", **kwargs)

        target_ops = replica_shell.os_ops
        target_port = replica_shell.pg.port
        target_base = replica_shell.pg.base_dir

        # create_node() constructs a placeholder PostgresNode so PgLab can
        # allocate the namespace, cgroup, address, port and storage location
        # before replication.  Reuse that base directory for the target-side
        # base backup; this keeps replica data under PgLab.base_dir (important
        # when PGLAB_TEST_ROOT selects a block-backed filesystem for I/O tests).
        try:
            spawned = primary.pg.replicate(
                name=name,
                target_os_ops=target_ops,
                target_host=ip,
                target_port=target_port,
                username=self.db_superuser,
                base_dir=target_base,
            )
        except Exception:
            self.remove_node(name)
            raise

        replica_shell.pg = spawned
        self._ensure_node_ownership(replica_shell)
        return replica_shell

    def network_down(self, name):
        node = self.nodes[name]
        _run(["ip", "-n", node.namespace, "link", "set", node.veth_ns, "down"])

    def network_up(self, name):
        node = self.nodes[name]
        _run(["ip", "-n", node.namespace, "link", "set", node.veth_ns, "up"])

    @staticmethod
    def _percent(value, name):
        value = float(value)
        if value < 0.0 or value > 100.0:
            raise PgLabError("{} must be between 0 and 100".format(name))
        return value

    def netem(
        self,
        name,
        delay_ms=None,
        jitter_ms=None,
        loss_pct=None,
        duplicate_pct=None,
        corrupt_pct=None,
        reorder_pct=None,
        rate_kbit=None,
    ):
        """Replace the node's root qdisc with a tc-netem fault profile.

        The profile is applied to traffic leaving the node's eth0 inside its
        network namespace. Calling netem() again replaces the previous profile.

        Examples:
            lab.netem("r1", delay_ms=100)
            lab.netem("r1", delay_ms=100, jitter_ms=20, loss_pct=5)
            lab.netem("r1", rate_kbit=1024)
            lab.clear_netem("r1")
        """
        node = self.nodes[name]
        _which("tc")

        args = [
            "ip", "netns", "exec", node.namespace,
            "tc", "qdisc", "replace", "dev", node.veth_ns,
            "root", "netem",
        ]

        have_fault = False

        if delay_ms is not None:
            delay_ms = float(delay_ms)
            if delay_ms < 0:
                raise PgLabError("delay_ms must be >= 0")
            args += ["delay", "{}ms".format(delay_ms)]
            have_fault = True

            if jitter_ms is not None:
                jitter_ms = float(jitter_ms)
                if jitter_ms < 0:
                    raise PgLabError("jitter_ms must be >= 0")
                args += ["{}ms".format(jitter_ms)]
        elif jitter_ms is not None:
            raise PgLabError("jitter_ms requires delay_ms")

        if loss_pct is not None:
            loss_pct = self._percent(loss_pct, "loss_pct")
            args += ["loss", "{}%".format(loss_pct)]
            have_fault = True

        if duplicate_pct is not None:
            duplicate_pct = self._percent(duplicate_pct, "duplicate_pct")
            args += ["duplicate", "{}%".format(duplicate_pct)]
            have_fault = True

        if corrupt_pct is not None:
            corrupt_pct = self._percent(corrupt_pct, "corrupt_pct")
            args += ["corrupt", "{}%".format(corrupt_pct)]
            have_fault = True

        if reorder_pct is not None:
            reorder_pct = self._percent(reorder_pct, "reorder_pct")
            if delay_ms is None:
                raise PgLabError("reorder_pct requires delay_ms")
            args += ["reorder", "{}%".format(reorder_pct)]
            have_fault = True

        if rate_kbit is not None:
            rate_kbit = int(rate_kbit)
            if rate_kbit <= 0:
                raise PgLabError("rate_kbit must be > 0")
            args += ["rate", "{}kbit".format(rate_kbit)]
            have_fault = True

        if not have_fault:
            raise PgLabError("netem requires at least one fault parameter")

        _run(args)

    def clear_netem(self, name):
        """Remove the root qdisc fault profile from a node, if present."""
        node = self.nodes[name]
        if shutil.which("tc") is None:
            return
        subprocess.run(
            [
                "ip", "netns", "exec", node.namespace,
                "tc", "qdisc", "del", "dev", node.veth_ns, "root",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )

    def _node_cgroup_pids(self, node):
        membership = Path(node.os_ops.cgroup_membership_file)
        try:
            lines = membership.read_text().splitlines()
        except FileNotFoundError:
            return []
        pids = []
        for line in lines:
            line = line.strip()
            if line.isdigit():
                pids.append(int(line))
        return sorted(set(pids))

    def freeze(self, name):
        """SIGSTOP every task currently in the node's PostgreSQL cgroup.

        Unlike a network partition this leaves sockets and processes alive, but
        the PostgreSQL node stops getting CPU time.  This models a wedged VM,
        scheduler stall, or process group that has stopped responding.
        """
        node = self.nodes[name]
        pids = self._node_cgroup_pids(node)
        if not pids:
            raise PgLabError("node cgroup has no processes to freeze: {}".format(name))

        stopped = []
        try:
            for pid in pids:
                try:
                    os.kill(pid, signal.SIGSTOP)
                    stopped.append(pid)
                except ProcessLookupError:
                    pass
        except Exception:
            # Do not leave a partially frozen node after a failed injection.
            for pid in stopped:
                try:
                    os.kill(pid, signal.SIGCONT)
                except ProcessLookupError:
                    pass
            raise

    def unfreeze(self, name):
        """SIGCONT all tasks currently present in the node cgroup."""
        node = self.nodes[name]
        for pid in self._node_cgroup_pids(node):
            try:
                os.kill(pid, signal.SIGCONT)
            except ProcessLookupError:
                pass

    def partition(self, left, right):
        lnode = self.nodes[left]
        rnode = self.nodes[right]
        _run(["ip", "-n", lnode.namespace, "route", "replace", "blackhole",
              "{}/32".format(rnode.ip)])
        _run(["ip", "-n", rnode.namespace, "route", "replace", "blackhole",
              "{}/32".format(lnode.ip)])

    def heal_partition(self, left, right):
        lnode = self.nodes[left]
        rnode = self.nodes[right]
        subprocess.run(["ip", "-n", lnode.namespace, "route", "del", "blackhole",
                        "{}/32".format(rnode.ip)], check=False)
        subprocess.run(["ip", "-n", rnode.namespace, "route", "del", "blackhole",
                        "{}/32".format(lnode.ip)], check=False)

    def set_io_limits(self, name, limits, io_device=None):
        node = self.nodes[name]
        devices = [io_device] if io_device else self._backing_block_devices(Path(node.pg.base_dir))
        self._apply_io_limits(node.cgroup, devices, limits)

    def remove_node(self, name):
        node = self.nodes.get(name)
        if node is None:
            return

        # A frozen PostgreSQL cannot cooperate with pg_ctl stop/cleanup.
        # Restore process scheduling and remove traffic shaping first.
        try:
            self.unfreeze(name)
        except Exception:
            pass
        try:
            self.clear_netem(name)
        except Exception:
            pass

        # PgLab removes the whole node directory itself below.  Using
        # PostgresNode.cleanup() here would perform another stop attempt first.
        # That is noisy for two intentional PgLab states: an uninitialized node
        # and a postmaster that a fault-injection test already killed.  stop()
        # itself always stops Testgres' log reader in its finally block, so call
        # it directly, tolerate the expected failure, and release Testgres-owned
        # resources before removing the isolation objects.
        try:
            node.pg.stop()
        except Exception:
            pass
        try:
            node.pg.release_resources()
        except Exception:
            pass

        subprocess.run(["ip", "netns", "del", node.namespace],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(["ip", "link", "del", node.veth_host],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.cgroup_backend.kill_node(node.cgroup)
        try:
            self.cgroup_backend.remove_node(node.cgroup)
        except OSError:
            pass
        if not self.keep_data:
            shutil.rmtree(str(self.base_dir / name), ignore_errors=True)
        del self.nodes[name]

    def close(self):
        if self._closed:
            return
        for name in list(self.nodes.keys()):
            self.remove_node(name)
        self._cleanup_firewall()
        if self._created_bridge:
            subprocess.run(["ip", "link", "del", self.bridge_name],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.cgroup_backend.cleanup_root()
        if not self.keep_data:
            try:
                self.base_dir.rmdir()
            except OSError:
                pass
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

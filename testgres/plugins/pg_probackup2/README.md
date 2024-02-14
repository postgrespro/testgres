# testgres - pg_probackup2

Ccontrol and testing utility for [pg_probackup2](https://github.com/postgrespro/pg_probackup). Python 3.5+ is supported.


## Installation

To install `testgres`, run:

```
pip install testgres-pg_probackup
```

We encourage you to use `virtualenv` for your testing environment.
The package requires testgres~=1.9.3.

## Usage

### Environment variables

| Variable | Required | Default value | Description |
| - | - | - | - |
| PGPROBACKUP_TMP_DIR | No | tests/tmp_dirs | The root of the temporary directory hierarchy where tests store data and logs. Relative paths start from the current working directory. |
| PG_PROBACKUP_TEST_BACKUP_DIR_PREFIX | No | Temporary test hierarchy | Prefix of the test backup directories. Must be an absolute path. Use this variable to store test backups in a location other than the temporary test hierarchy. |

See [Testgres](https://github.com/postgrespro/testgres/tree/master#environment) on how to configure a custom Postgres installation using `PG_CONFIG` and `PG_BIN` environment variables.

### Examples

Here is an example of what you can do with `testgres-pg_probackup2`:

```python
# You can see full script here plugins/pg_probackup2/pg_probackup2/tests/basic_test.py
def test_full_backup(self):
    # Setting up a simple test node
    node = self.pg_node.make_simple('node', pg_options={"fsync": "off", "synchronous_commit": "off"})

    # Initialize and configure Probackup
    self.pb.init()
    self.pb.add_instance('node', node)
    self.pb.set_archiving('node', node)

    # Start the node and initialize pgbench
    node.slow_start()
    node.pgbench_init(scale=100, no_vacuum=True)

    # Perform backup and validation
    backup_id = self.pb.backup_node('node', node)
    out = self.pb.validate('node', backup_id)

    # Check if the backup is valid
    self.assertIn(f"INFO: Backup {backup_id} is valid", out)
```

## Authors

[Postgres Professional](https://postgrespro.ru/about)

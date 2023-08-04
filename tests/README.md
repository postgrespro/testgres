### How do I run tests?

#### Simple

```bash
# Setup virtualenv
virtualenv venv
source venv/bin/activate

# Install local version of testgres
pip install -U .

# Set path to PostgreSQL
export PG_BIN=/path/to/pg/bin

# Run tests
./tests/test_simple.py
```

#### All configurations + coverage

```bash
# Set path to PostgreSQL and python version
export PATH=/path/to/pg/bin:$PATH
export PYTHON_VERSION=3  # or 2

# Run tests
./run_tests.sh
```


#### Remote host tests

1. Start remote host or docker container
2. Make sure that you run ssh
```commandline
sudo apt-get install openssh-server
sudo systemctl start sshd
```
3. You need to connect to the remote host at least once to add it to the known hosts file
4. Generate ssh keys
5. Set up params for tests


```commandline
conn_params = ConnectionParams(
    host='remote_host',
    username='username',
    ssh_key=/path/to/your/ssh/key'
)
os_ops = RemoteOperations(conn_params)
```
If you have different path to `PG_CONFIG` on your local and remote host you can set up `PG_CONFIG_REMOTE`, this value will be
using during work with remote host.

`test_remote` - Tests for RemoteOperations class.

`test_simple_remote` - Tests that create node and check it. The same as `test_simple`, but for remote node. 
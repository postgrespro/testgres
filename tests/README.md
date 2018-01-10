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
export PATH=$PATH:/path/to/pg/bin
export PYTHON_VERSION=3  # or 2

# Run tests
./run_tests.sh
```

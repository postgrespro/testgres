### How do I run tests?

#### Simple

```bash
# Setup virtualenv
virtualenv venv
source venv/bin/activate

pip install -r tests/requirements.txt

# Set path to PostgreSQL
export PG_BIN=/path/to/pg/bin

# Run tests
pytest -l -v -n 4 tests
```

#### All configurations + coverage

```bash
# Set path to PostgreSQL and python version
export PATH=/path/to/pg/bin:$PATH

# Set path of python binary
export PYTHON_BINARY=python3

# Run tests
./run_tests.sh
```

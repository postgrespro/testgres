# testgres

Postgres testing utility

## Installation

`TODO`

## Usage

```
from testgres import get_new_node

try:
    node = get_new_node('test')
    node.init()
    node.start()
    stdout = node.psql('postgres', 'SELECT 1')
    print stdout
    node.stop()
except ClusterException, e:
    node.cleanup()
```

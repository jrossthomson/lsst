"""TODO(jrossthomson): DO NOT SUBMIT without one-line documentation for task.

TODO(jrossthomson): DO NOT SUBMIT without a detailed description of task.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from absl import app
from absl import flags

FLAGS = flags.FLAGS

schema = [
    bigquery.SchemaField('full_name', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('age', 'INTEGER', mode='REQUIRED'),
]

table_ref = dataset_ref.table('my_table')
table = bigquery.Table(table_ref, schema=schema)
table = client.create_table(table)  # API request

assert table.table_id == 'my_table'

def main(argv):
  if len(argv) > 1:
    raise app.UsageError('Too many command-line arguments.')

if __name__ == '__main__':
  app.run(main)

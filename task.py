

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

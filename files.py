import bz2
import re
import requests
import shutil
import urllib
from google.cloud import storage
from google.cloud import bigquery


def download_file(src_url, dest_file):
  r = requests.get(src_url, stream=True)
  with open(dest_file, 'wb') as f:
    shutil.copyfileobj(r.raw, f)
  print('Downloaded {} to {}.'.format(src_url, dest_file))

def bunzip2(src_file, dest_file):
  with open(src_file, 'rb') as source, open(dest_file, 'wb') as dest:
    dest.write(bz2.decompress(source.read()))
  print('File {} bunziped to {}.'.format(src_file, dest_file))

def bunzip2_mem(src_file, dest_file):
  print src_file
  print dest_file
  zipfile = bz2.BZ2File(src_file) # open the file
  data = zipfile.read() # get the decompressed data
  open(dest_file, 'wb').write(data) # write a uncompressed file
  print('File {} bunziped to {}.'.format(src_file, dest_file))

def load2bq(uri='', 
        dataset_id='LSST', 
        table_id='mep_wise',
        delimiter='|'):
  client = bigquery.Client()

  dataset_ref = client.dataset(dataset_id)
  table_ref = dataset_ref.table(table_id)
  table = client.get_table(table_ref)  # API Request

  dataset_ref = client.dataset(dataset_id)
  job_config = bigquery.LoadJobConfig()
  job_config.schema = table.schema
  job_config.source_format = bigquery.SourceFormat.CSV
  job_config.field_delimiter = delimiter
  job_config.create_disposition = 'CREATE_IF_NEEDED'

  load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)  # API request

  print('Starting job {}'.format(load_job.job_id))
  load_job.result()  # Waits for table load to complete.
  print('Job finished.')

  print('Loaded table now has {} rows.'.format(table.num_rows))

def gcs_remove(bucket_name, blob_name):
  """Uploads a file to the bucket."""
  storage_client = storage.Client()
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(blob_name)
  blob.delete()
  print('File {} removed from GCS.'.format(blob_name))

def upload2gcs(bucket_name, src_file, dest_blob):
  """Uploads a file to the bucket."""
  storage_client = storage.Client()
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(dest_blob)
  blob.upload_from_filename(src_file)
  print('File {} uploaded to GCS: {}.'.format(src_file, dest_blob))

def gethttpfilesize(url):
    print "opening url:", url
    site = urllib.urlopen(url)
    meta = site.info()
    print "Content-Length:", meta.getheaders("Content-Length")[0]


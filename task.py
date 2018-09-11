import files as files
import sys
import os
from urlparse import urlparse

from absl import app
from absl import flags

FLAGS = flags.FLAGS

flags.DEFINE_string('uri', '', 'URI from whence to download the data.')
flags.DEFINE_bool('download', True, 'Download from web')
flags.DEFINE_bool('bunzip2', True, 'Bunzip the files')
flags.DEFINE_bool('gcs_upload', True, 'Upload to Google Cloud Storage')
flags.DEFINE_bool('bq_load', True, 'Load to Google Cloud BigQuery Table')
flags.DEFINE_bool('cleanup', True, 'If true, removes files')
flags.DEFINE_string('bucket', 'lsst-mep', 'Name of Google Cloud Storage Bucket')
flags.DEFINE_string('dataset_id', 'LSST', 'Name of Google Cloud BigQuery Dataset')
flags.DEFINE_string('table_id', 'mep-wise', 'Name of Google Cloud BigQuery Table')


def main(argv):
  # Parse the URL
  the_url = FLAGS.uri
  parsed_url = urlparse(the_url)
  the_path = parsed_url.path
  raw_filename = os.path.split(the_path)[-1]
  filename, file_extension = os.path.splitext(raw_filename)

  # Do the work if the flags allow

  if FLAGS.download: files.download_file(the_url, raw_filename)
  if FLAGS.bunzip2: files.bunzip2(raw_filename, filename)
  if FLAGS.gcs_upload: files.upload2gcs(FLAGS.bucket, filename, filename)
  if FLAGS.bq_load: files.load2bq(uri='gs://' + FLAGS.bucket + '/' + filename,
      dataset_id='LSST',
      table_id='mep_wise')

  if FLAGS.cleanup: 
    try:
      os.remove(filename)
    except:
      pass
    try:
     os.remove(raw_filename)
    except:
      pass
    try:
     files.gcs_remove(FLAGS.bucket, filename)
    except:
      pass

if __name__ == '__main__':
  app.run(main)

import urllib
import bz2

def url2local(src_url, dest_file):
  f = urllib.URLopener()
  f.retrieve(src_url, dest_file)

def bunzip2(src_file, dest_file):
  zipfile = bz2.BZ2File(src_file) # open the file
  data = zipfile.read() # get the decompressed data
  open(dest_file, 'wb').write(data) # write a uncompressed file

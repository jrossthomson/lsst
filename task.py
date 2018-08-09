import files as files


files.url2local('https://storage.googleapis.com/lsst-wise/chick.png.bz2', 'chick.png.bz2')
files.bunzip2('chick.png.bz2', 'c.png')

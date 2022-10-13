import tempfile

import indexed_gzip

file_path = 'temp_10GB_file'

def test_indexed_gzip(file_path):
    source_fileobj = open(file_path, 'rb')
    output_fileobj = source_fileobj
    # do the same thing as SQLiteIndexedTar do
    tar_file = indexed_gzip.IndexedGzipFile(fileobj=output_fileobj, drop_handles=False, spacing=4*1024*1024)
    while tar_file.read(1024 * 1024):
        continue

test_indexed_gzip(file_path)  # filepath points to a large file.

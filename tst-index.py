import indexed_gzip

file_path = 'temp_10GB_file.gz'

def test_indexed_gzip(file_path):
    source_fileobj = open(file_path, 'rb')
    tar_file = indexed_gzip.IndexedGzipFile(fileobj=source_fileobj, drop_handles=False, spacing=4*1024*1024)
    while tar_file.read(1024 * 1024):
        continue

test_indexed_gzip(file_path)

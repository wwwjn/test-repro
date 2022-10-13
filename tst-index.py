import indexed_gzip
import io

file_path = 'temp_file.gz'

def test_indexed_gzip(file_path):
    source_fileobj = open(file_path, 'rb')
    def fn():
        raise io.UnsupportedOperation
    #source_fileobj.seek = fn
    #source_fileobj.tell = fn
    f = io.BytesIO(source_fileobj.read())
    f.seekable = lambda: False
    #source_fileobj.fileno = fn
    tar_file = indexed_gzip.IndexedGzipFile(fileobj=f)
    tar_file.build_full_index()
#     while tar_file.read(1024 * 1024):
#         continue

test_indexed_gzip(file_path)

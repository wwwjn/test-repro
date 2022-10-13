import tempfile
from codalab.worker.file_util import GzipStream

from codalab.lib.beam.SQLiteIndexedTar import SQLiteIndexedTar
import indexed_gzip

file_path = 'temp_10GB_file'

def test_indexed_gzip(file_path):
    source_fileobj = open(file_path, 'rb')
    output_fileobj = GzipStream(source_fileobj)
    # do the same thing as SQLiteIndexedTar do
    tar_file = indexed_gzip.IndexedGzipFile(fileobj=output_fileobj, drop_handles=False, spacing=4*1024*1024)
    while tar_file.read(1024 * 1024):
        continue
    # output_fileobj is not file-like 
    # so we download it from remote cloud storage again 
    # with tempfile.NamedTemporaryFile(suffix=".sqlite") as tmp_index_file:
    #     SQLiteIndexedTar(
    #         fileObject=output_fileobj,
    #         tarFileName="contents",  # If saving a single file as a .gz archive, this file can be accessed by the "/contents" entry in the index.
    #         writeIndex=True,
    #         clearIndexCache=True,
    #         indexFilePath=tmp_index_file.name,
    #         printDebug=3,
    #     )

test_indexed_gzip(file_path)  # filepath points to a large file.

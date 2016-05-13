#file/generic
import subprocess
import importlib
import sys
sys.path.insert(0, '../')

def fileParse(PATH_NAME,mime):

    dbManager = importlib.import_module("dbManager")
    dbmanager = dbManager.Manager()
    #es = elasticsearch.Elasticsearch("127.0.0.1:9200")

    p1 = subprocess.Popen(["exiftool", PATH_NAME], stdout=subprocess.PIPE)
    result = p1.communicate()[0]
    # Now process the result, getting the lines with values
    tokens = result.split('\n')
    # print tokens
    print 'File metadata---------------------------------------------'
    for token in tokens:
        if token != '':
            output = token.split(':', 1)
            print output[0].strip(" ")
            print output[1].strip(" ")
            doc = {
                "filePath": PATH_NAME,
                output[0].strip(" "): output[1].strip(" ")
            }
            dbmanager.push('forensic_db','file-metadata',doc)
            #es.index(index='forensic_db', doc_type='file-metadata', body=doc)

    return 0;
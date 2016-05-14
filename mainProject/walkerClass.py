import os
import time
import hashlib
import subprocess
import logging
import elasticsearch
import dbManager

# Dimension expressed in megabytes
BREACKPOINT_DIMENSION = 100;

class Walker:

    dbmanager = dbManager.Manager()
    #es = elasticsearch.Elasticsearch("127.0.0.1:9200")


    log = logging.getLogger("main.walkerClass")
    breackpointVar = 1
    dim_counter = 0


    def __init__(self,parser,breackpointFile):

        self.breackpointFile=breackpointFile
        self.parser=parser
    # To analyze the content of a folder, call this method and pass the path of a folder. It will automatically
    # retrieve all the files inside.

    def WalkPath(self,rootPath):
        for root, dirs, files in os.walk(rootPath):
            for file in files:
                fname = os.path.join(root, file)
                if os.path.isfile(fname):
                    # Now we have to perform some operation, like compute an hash, get metadata, if is a final file,
                    # or go deeper, if it is a compressed file.
                    self.getFileSystemMetaData(fname)



    # fname is a path to the desired file.
    def getFileSystemMetaData(self, fname):
        #print '***************'

        # Get mime type of the file
        p1 = subprocess.Popen(["xdg-mime", "query", "filetype", fname],stdout=subprocess.PIPE)
        mime = p1.communicate()[0]
        mime = str(mime).strip()

        # Get filesystem meta-data+hash
        (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat(fname)
        modifiedTime = time.ctime(mtime)
        accessTime = time.ctime(atime)
        createdTime = time.ctime(ctime)
        fileHash = hashlib.md5(open(fname, 'rb').read()).hexdigest()
        # print 'relative path: ' + fname
        # print 'extension: ' + str(os.path.splitext(fname)[1])
        # print 'mime type: ' + mime
        # print 'size: '+ str(size)
        # print 'modifiedTime: ' + str(modifiedTime)
        # print 'accessTime: ' + str(accessTime)
        # print 'createdTime: ' + str(createdTime)
        # print 'hash: ' + str(fileHash)

        #Loading into kibana
        doc = {
            "filePath": fname,
            "extension": str(os.path.splitext(fname)[1])
        }
        self.dbmanager.push('forensic_db','file-system-metadata',doc)
        #self.es.index(index='forensic_db', doc_type='file-system-metadata', body=doc)

        doc = {
            "filePath": fname,
            "mimeType": mime
        }
        self.dbmanager.push('forensic_db', 'file-system-metadata', doc)
        #self.es.index(index='forensic_db', doc_type='file-system-metadata', body=doc)

        doc = {
            "filePath": fname,
            "size": str(size)
        }
        self.dbmanager.push('forensic_db', 'file-system-metadata', doc)
        #self.es.index(index='forensic_db', doc_type='file-system-metadata', body=doc)

        doc = {
            "filePath": fname,
            "modifiedTime": str(modifiedTime)
        }
        self.dbmanager.push('forensic_db', 'file-system-metadata', doc)
        #self.es.index(index='forensic_db', doc_type='file-system-metadata', body=doc)

        doc = {
            "filePath": fname,
            "accessTime": str(accessTime)
        }
        self.dbmanager.push('forensic_db', 'file-system-metadata', doc)
        #self.es.index(index='forensic_db', doc_type='file-system-metadata', body=doc)

        doc = {
            "filePath": fname,
            "accessTime": str(createdTime)
        }
        self.dbmanager.push('forensic_db', 'file-system-metadata', doc)

        doc = {
            "filePath": fname,
            "createdTime": str(fileHash)
        }
        self.dbmanager.push('forensic_db', 'file-system-metadata', doc)
        #self.es.index(index='forensic_db', doc_type='file-system-metadata', body=doc)

        if self.breackpointFile == '###':
            self.breackpointVar = 0
        elif self.breackpointVar == 1 and self.breackpointFile != fname:
            return
        else:
            self.breackpointVar = 0

        self.getFileMetadata(mime, fname)




    def getFileMetadata(self,mime,fname):

        dimNextFileToParse = os.path.getsize(fname)
        if "/media/temp/" not in fname:
            # dimension in Byte
            if self.dim_counter + dimNextFileToParse > BREACKPOINT_DIMENSION * 1000000:
                self.log.info('[BREAKPOINT] #' + fname)
                self.dim_counter = 0;
                print 'BREAKPOINT SETTED'
            else:
                self.dim_counter = self.dim_counter + dimNextFileToParse
        self.parser.parse(mime, fname)

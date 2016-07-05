import os
import time
import hashlib
import subprocess
import logging
import dbManager

# Dimension expressed in megabytes
BREACKPOINT_DIMENSION = 100

class Walker:

    dbmanager = dbManager.dbManager.get_instance()


    log = logging.getLogger("main.walkerClass")

    breackpointVar = 1
    dim_counter = 0


    def __init__(self,parser,breackpointFile):

        self.breackpointFile=breackpointFile
        self.parser=parser
    # To analyze the content of a folder, call this method and pass the path of a folder. It will automatically
    # retrieve all the files inside.

    #The comingPath variable (if setted) is useful to not forget the archive filePath we are exploring.
    def WalkPath(self,rootPath,comingPath="",recursiveFlag=""):
        for root, dirs, files in os.walk(rootPath):
            for file in files:
                fname = os.path.join(root, file)
                if os.path.isfile(fname):
                    # Now we have to perform some operation, like compute an hash, get metadata, if is a final file,
                    # or go deeper, if it is a compressed file.
                    self.getFileSystemMetaData(fname,comingPath,recursiveFlag)



    # fname is a path to the desired file.
    def getFileSystemMetaData(self, fname, comingPath="",recursiveFlag=""):
        #print '***************'
        if self.breackpointFile == '###':
            self.breackpointVar = 0
        elif self.breackpointVar == 1 and self.breackpointFile != fname:
            print fname,' Skipped'
            return
        else:
            self.breackpointVar = 0

        properties = {}
         # Get mime type of the file
        p1 = subprocess.Popen(["xdg-mime", "query", "filetype", fname],stdout=subprocess.PIPE)
        mime = p1.communicate()[0]
        mime = str(mime).strip()

        if os.path.splitext(fname)[1] == ".pst":
            mime = "application/pst"

        properties['mime']=mime

        # Get filesystem meta-data+hash
        (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat(fname)
        modifiedTime = time.ctime(mtime)
        properties['modifiedTime']=modifiedTime
        accessTime = time.ctime(atime)
        properties['accessTime']=accessTime
        createdTime = time.ctime(ctime)
        properties['createdTime']=createdTime
        fileHash = hashlib.md5(open(fname, 'rb').read()).hexdigest()
        properties['fileHash']=fileHash
        properties['size'] = size
        properties['extension'] = str(os.path.splitext(fname)[1])
        # print 'relative path: ' + fname
        # print 'extension: ' + str(os.path.splitext(fname)[1])
        # print 'mime type: ' + mime
        # print 'size: '+ str(size)
        # print 'modifiedTime: ' + str(modifiedTime)
        # print 'accessTime: ' + str(accessTime)
        # print 'createdTime: ' + str(createdTime)
        # print 'hash: ' + str(fileHash)


        if comingPath == "":
            realPath=fname
        else:
            realPath=comingPath+"/"+os.path.basename(fname)

        actions=[]

        for key,value in properties.iteritems():

            action = {
                "_index":"forensic_db",
                "_type":"file-system-metadata",
                "_source": {
                    "filePath":realPath,
                    key:value
                }

            }

            actions.append(action)

        self.dbmanager.bulk(actions)
        if recursiveFlag == "":
            if comingPath == "":
                self.getFileMetadata(mime, fname)
            else:
                self.getFileMetadata(mime, fname, realPath)
        else:
            doc = {
                    "filePath": realPath,
                    "exception": "File-Metadata of file inside archive not retrieved"
            }
            self.dbmanager.push('forensic_db','exception',doc)



    def getFileMetadata(self,mime,fname,path=""):

        if path == "":
            filepath=fname
        else:
            filepath = path

        dimNextFileToParse = os.path.getsize(fname)
        if "/media/temp/" not in fname:
            # dimension in Byte
            if self.dim_counter + dimNextFileToParse > BREACKPOINT_DIMENSION * 1000000:
                self.log.info('[BREAKPOINT] #' + fname)
                self.dim_counter = 0;
                print 'BREAKPOINT SETTED'
            else:
                self.dim_counter = self.dim_counter + dimNextFileToParse
        try:
            print 'Parsing the file',fname
            self.parser.parse(mime, fname, path)
        except Exception,e:
            print str(e)
            print 'exception in getFileMetadata! for file',fname
            doc = {
                "filePath": filepath,
                "exception": "Error in parsing the file",
            }
            self.dbmanager.push('forensic_db', 'file-system-metadata', doc)


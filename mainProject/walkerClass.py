import os
import time
import hashlib
import subprocess
import utils
import logging
import dbManager

class Walker:

    dbmanager = dbManager.dbManager.get_instance()
    util = utils.utils.get_instance()


    log = logging.getLogger("main.walkerClass")

    breackpointVar = 1
    dim_counter = 0

    def __init__(self, parser, breakpointFile):

        self.breakpointFile=breakpointFile
        self.parser=parser

    # To analyze the content of a folder, call this method and pass the path of a folder. It will automatically
    # retrieve all the files inside.

    # The comingPath variable (if setted ) is useful to not forget the archive filePath we are exploring.
    def WalkPath(self,rootPath,comingPath=""):
        for root, dirs, files in os.walk(rootPath):
            for file in files:
                fname = os.path.join(root, file)
                if os.path.isfile(fname):

                    # Now we have to perform some operation, like compute an hash, get metadata, if is a final file,
                    # or go deeper, if it is a compressed file.
                    self.getFileSystemMetaData(fname,comingPath)

    # fname is a path to the desired file
    def getFileSystemMetaData(self, fname, comingPath=""):

        # Check if some file should not be analized.
        if self.breakpointFile == '###':
            self.breackpointVar = 0
        elif self.breackpointVar == 1 and self.breakpointFile != fname:
            print fname,' Skipped'
            return
        else:
            self.breackpointVar = 0

        properties = {}

        # Get mime type of the file.
        p1 = subprocess.Popen(["xdg-mime", "query", "filetype", fname],stdout=subprocess.PIPE)
        mime = p1.communicate()[0]
        mime = str(mime).strip()

        # Check for the file extension, useful to understand if it is a pst.
        if os.path.splitext(fname)[1] == ".pst":
            mime = "application/pst"

        properties['mime']=mime

        # Get filesystem meta-data + hash
        (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat(fname)
        modifiedTime = time.ctime(mtime)
        properties['modifiedTime']=modifiedTime
        accessTime = time.ctime(atime)
        properties['accessTime']=accessTime
        createdTime = time.ctime(ctime)
        properties['createdTime']=createdTime
        #fileHash = hashlib.md5(open(fname, 'rb').read()).hexdigest()
        #properties['fileHash']=fileHash
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

        # Build the action[] list to post to database
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


        if comingPath == "":
            self.getFileMetadata(mime, fname)
        else:
            self.getFileMetadata(mime, fname, realPath)

    # Method used to get file-metadata
    def getFileMetadata(self, mime, fname, realpPath=""):

        if realpPath == "":
            filepath=fname
        else:
            filepath = realpPath

        # We have now that filepath is the actual path of the file (that will be stored on elasticsearch), while fname
        # is the path of the file to be analized

        dimNextFileToParse = os.path.getsize(fname)

        # Not place breakpoints inside archives
        if self.util.getTempDirArchives() not in fname:

            # Check if a breakpoint should be placed before parsing the file
            a = self.util.checkBreakPoint(dimNextFileToParse)
            if a == 0:
                self.log.info('[BREAKPOINT] #' + filepath)
                print 'BREAKPOINT SETTED'

        try:
            print 'Parsing the file',filepath
            self.parser.parse(mime, fname, realpPath)
        except Exception,e:

            # Error while parsing the file
            actions = []
            print str(e)
            print 'exception in getFileMetadata! for file',fname

            action = {
                "_index": "forensic_db",
                "_type": "file-system-metadata",
                "_source": {
                    "filePath": filepath,
                    "exception": "Error in parsing the file",
                }

            }

            actions.append(action)

            self.dbmanager.bulk(actions)


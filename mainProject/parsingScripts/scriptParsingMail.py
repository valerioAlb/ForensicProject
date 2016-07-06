#application/mbox message/rfc822 application/pst
from email.parser import Parser
import mailbox
import subprocess
import rarfile
import zipfile
import io
import os
import tempfile
import importlib
import shutil
import sys
sys.path.insert(0, '../')

dbManager = importlib.import_module("dbManager")
dbmanager = dbManager.dbManager.get_instance()

utils = importlib.import_module("utils")
util = utils.utils.get_instance()

OUTPUT_PATH = util.getTempDirArchives()

def fileParse(PATH_NAME,extension,realPath=""):

    #PATH_NAME is the current position of the file to analize
    #path is the position of the original file that is going to be analized. For example a file inside a rar
    #has a PATH_NAME which is: /tmp/filename while path is /rarFileName/filename.

    if realPath=="":
        path = PATH_NAME
    else:
        path = realPath

    if extension == 'message/rfc822':

        parseSingleMail(PATH_NAME, path, realPath)# Here parsing for mailboxes

    if extension == 'application/mbox':

        parseMailBox(PATH_NAME, path, realPath)

    if extension == 'application/pst':

        tempDir = OUTPUT_PATH + os.path.basename(PATH_NAME) + "_pst"
        if not os.path.exists(tempDir):
            os.mkdir(tempDir)
        parsePST(PATH_NAME,tempDir)
        p1 = subprocess.Popen(["rm","-r", tempDir], stdout=subprocess.PIPE)
        p1.communicate()

    print 'All Mails parsed.'

    return 0


def parsePST(PATH_NAME,tempDir):
    print 'Conversion of file PST: ' + PATH_NAME
    convert_pst_to_mbox(PATH_NAME, tempDir)
    for root, dirs, files in os.walk(tempDir):
        for file in files:
            fname = os.path.join(root, file)
            if os.path.isfile(fname):
                print 'Analizing mailbox: ' + fname

                parseMailBox(fname, PATH_NAME + file, PATH_NAME + file)




def parseMailBox(PATH_NAME, path, realPath):
    mbox = mailbox.mbox(PATH_NAME)
    numMail = len(mbox)
    j = 1
    print 'parsing mailbox ' + PATH_NAME
    print 'mailbox ' + PATH_NAME + ' has ' + str(numMail) + ' elements'
    for message in mbox:
        actions = []

        ID = message['Message-ID']

        action = {
            "_index": "forensic_db",
            "_type": "file-metadata",
            "_source": {
                'mailID': ID,
                'filePath': path,
            }

        }

        actions.append(action)

        for x in message.items():
            action = {
                "_index": "forensic_db",
                "_type": "mail",
                "_source": {
                    'mailID': ID,
                    x[0].replace(".", "_"): x[1],
                }

            }

            actions.append(action)

        dbmanager.bulk(actions)

        # From now on we consider the mail attachments

        for part in message.walk():
            parseMailAttachment(ID, PATH_NAME, part, realPath)

        #print 'Parse message', j
        if (j % 100 == 0):
            print('Parsed ' + str(j) + " mails of " + str(numMail) + ' for mailbox ' + PATH_NAME)
        j = j + 1


def parseSingleMail(PATH_NAME, path, realPath):
    actions = []
    message = Parser().parse(open(PATH_NAME, 'r'))
    ID = message['Message-ID']
    action = {
        "_index": "forensic_db",
        "_type": "file-metadata",
        "_source": {
            'mailID': ID,
            'filePath': path,
        }

    }
    actions.append(action)
    for x in message.items():
        action = {
            "_index": "forensic_db",
            "_type": "mail",
            "_source": {
                'mailID': ID,
                x[0].replace(".", "_"): x[1],
            }

        }

        actions.append(action)
    dbmanager.bulk(actions)
    for part in message.walk():
        parseMailAttachment(ID, PATH_NAME, part, realPath)


def parseMailAttachment(ID, PATH_NAME, part ,realPath=""):

    actions = []

    if realPath == "":
        path = PATH_NAME
    else:
        path = realPath

    name = part.get_filename()

    ftype = part.get_content_type()

    if ftype == 'application/zip':
        data = part.get_payload()
        decodedData = data.decode('base64')
        f = io.BytesIO(decodedData)
        archive = zipfile.ZipFile(f)
        for file in archive.infolist():
            properties = {}

            filePath = path + '/' + str(ID) + '/' + str(name)+'/'+str(file.filename)
            zipFileName = file.filename
            properties["zipFileName"] = zipFileName
            zipFileSize = file.file_size
            properties["zipFileSize"] = zipFileSize
            zipCompressSize = file.compress_size
            properties["zipCompressSize"] = zipCompressSize
            zipCreateSystem = file.create_system
            properties["zipCreateSystem"] = zipCreateSystem
            zipInternalAttr = file.internal_attr
            properties["zipInternalAttr"] = zipInternalAttr
            zipExternalAttr = file.external_attr
            properties["zipExternalAttr"] = zipExternalAttr
            zipDateTime = file.date_time
            properties["zipDateTime"] = zipDateTime

            uploadDatabase(properties,filePath)

        f.close()
    elif ftype == 'application/rar':
        data = part.get_payload()
        decodedData = data.decode('base64')
        f = io.BytesIO(decodedData)
        archive = rarfile.RarFile(f)
        for file in archive.infolist():
            properties = {}

            filePath = path + '/' + str(ID) + '/' + str(name) + '/' + str(file.filename)
            rarFileName = file.filename
            properties["rarFileName"] = rarFileName
            rarFileSize = file.file_size
            properties["rarFileSize"] = rarFileSize
            rarCompressSize = file.compress_size
            properties["rarCompressSize"] = rarCompressSize
            rarDateTime = file.date_time
            properties["rarDateTime"] = rarDateTime

            uploadDatabase(properties,filePath)

        f.close()

    elif ftype == 'text/plain':
        filePath = path + '/' + str(ID)
        body = part.get_payload()  # to control automatic email-style MIME decoding (e.g., Base64, uuencode, quoted-printable)

        action = {
            "_index": "forensic_db",
            "_type": "mails",
            "_source": {
                'mailID': ID,
                'text/plain': unicode(body, errors='ignore'),
            }
        }

        actions.append(action)
        dbmanager.bulk(actions)

    elif ftype == 'text/html':

        body = part.get_payload()  # to control automatic email-style MIME decoding (e.g., Base64, uuencode, quoted-printable)

        action = {
            "_index": "forensic_db",
            "_type": "mails",
            "_source": {
                'mailID': ID,
                'text/html': unicode(body, errors='ignore'),
            }

        }

        actions.append(action)
        dbmanager.bulk(actions)

    elif 'multipart/' in ftype:
        #nothing to do.
        pass
    else:
        try:
            filePath = path + '/' + str(ID) + '/' + str(name)
            #print 'Parsing mail content',filePath
            data = part.get_payload()
            decodedData = data.decode('base64')
            # ###
            temp = tempfile.NamedTemporaryFile()
            temp.write(decodedData)
            temp.seek(0)
            p1 = subprocess.Popen(["exiftool", temp.name], stdout=subprocess.PIPE)
            result = p1.communicate()[0]
            tokens = result.split('\n')
            # # print tokens
            # #print 'File metadata---------------------------------------------'

            actions = []
            #
            for token in tokens:
                 if token != '':
                    output = token.split(':', 1)

                    action = {
                        "_index": "forensic_db",
                        "_type": "mail",
                        "_source": {
                             'filePath': filePath,
                             output[0].strip(" ").replace(".", "_"): output[1].strip(" "),
                         }

                     }

                    actions.append(action)

                 dbmanager.bulk(actions)
        except:

            actions = []
            print 'Problem with file parsing in a mail'

            path = PATH_NAME + '/' + str(ID) + '/' + str(name)

            action = {
                "_index": "forensic_db",
                "_type": "mail",
                "_source": {
                    'mailID': ID,
                    "exception": "Problem with file parsing: " + path,
                }

            }

            actions.append(action)

            dbmanager.bulk(actions)
        finally:
        #     # Automatically cleans up the file
             try:
                 temp.close()
             except:
                 pass


def uploadDatabase(properties,path):

    actions = []

    for key, value in properties.iteritems():
        action = {
            "_index": "forensic_db",
            "_type": "file-system-metadata",
            "_source": {
                "filePath": path,
                key: value
            }

        }

        actions.append(action)

    dbmanager.bulk(actions)

def convert_pst_to_mbox(pstfilename, outputfolder):
    print 'Starting conversion into folder '+outputfolder+ ' of the PST: '+pstfilename
    subprocess.call(['readpst', '-o', outputfolder, '-D', '-q', pstfilename])
    print 'Conversion done'
#application/mbox message/rfc822 application/pst
# -*- coding: utf-8 -*-
from email.parser import Parser
import email.utils
from langdetect import detect
import mailbox
import subprocess
import rarfile
import datetime
import time
import zipfile
import io
import os
import chardet
import tempfile
import importlib
import sys
from time import sleep
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

        parseSingleMail(PATH_NAME, path)# Here parsing for mailboxes

    if extension == 'application/mbox':

        parseMailBox(PATH_NAME, path)

    if extension == 'application/pst':

        tempDir = OUTPUT_PATH + os.path.basename(PATH_NAME) + "_pst/"
        if not os.path.exists(tempDir):
            os.mkdir(tempDir)
        parsePST(PATH_NAME,tempDir, path)
        p1 = subprocess.Popen(["rm","-r", tempDir], stdout=subprocess.PIPE)
        p1.communicate()

    #print 'All Mails parsed.'

    return 0


def parsePST(PATH_NAME,tempDir, path):

    print 'Conversion of file PST: ' + path
    convert_pst_to_mbox(PATH_NAME, tempDir)
    for root, dirs, files in os.walk(tempDir):
        for file in files:
            fname = os.path.join(root, file)
            if os.path.isfile(fname):
                print 'Analyzing mailbox: ' + fname

                parseMailBox(fname, path+'/'+file)




def parseMailBox(PATH_NAME, path):

    mbox = mailbox.mbox(PATH_NAME)
    print 'parsing mailbox ' + path
    #print 'mailbox ' + path + ' has ' + str(numMail) + ' elements'
    for message in mbox:
        ID = parse_email_header(message, path)

        if ID is not None:
            for part in message.walk():
                    parseMailAttachment(ID, PATH_NAME, part, path)

def parseSingleMail(PATH_NAME, path):



    message = Parser().parse(open(PATH_NAME, 'r'))


    ID = parse_email_header(message, path)

    if ID is not None:

        for part in message.walk():
            parseMailAttachment(ID, PATH_NAME, part, path)


def parse_email_header(message, path):

    actions = []

    ID = message['Message-ID']

    if ID is None or ID is '':
        print '####################################################################################'
        print message
        print '####################################################################################'
        return None
    else:
        #ID = unicode(ID, 'utf8', errors='replace')
        print ID

    filepath = unicode(path, 'utf8', errors='replace')

    action = {
        "_index": util.getIndex(),
        "_type": "mail",
        "_source": {
            'mailID': ID,
            'filePath': filepath,
        }

    }
    actions.append(action)

    for x in message.items():

        field = unicode(x[0].replace(".", "_"), 'utf8', errors='replace')
        value = unicode(x[1], 'utf8', errors='replace')

        action = {
            "_index": util.getIndex(),
            "_type": "mail",
            "_source": {
                'mailID': ID,
                field: value,
            }

        }

        actions.append(action)

    ###################################### Manage DATE field ########################

    # Original Field, something like 'Fri, 15 Jul 2016 20:40:31 +0200'
    date_str = message.get('date')
    parsedDate = email.utils.parsedate_tz(date_str)
    # Convert to timezone 0
    timestamp = email.utils.formatdate(email.utils.mktime_tz(parsedDate))
    parsedDate = email.utils.parsedate(timestamp)
    finalDate = datetime.datetime.fromtimestamp(time.mktime(parsedDate))
    # Convert date in elasticsearch-recognized field, something like '2016-07-15T18:40:31'
    normalized_date_notime = str(finalDate).split(' ', 1)[0]
    # Convert date in elasticsearch-recognized field, something like '2016-07-15T18:40:31'
    finalDate = str(finalDate).replace(" ", "T")
    normalized_date = unicode(finalDate,'utf8',errors='replace')

    action = {
        "_index": util.getIndex(),
        "_type": "mail",
        "_source": {
            'mailID': ID,
            'normalized_date': normalized_date,
        }

    }
    actions.append(action)

    action = {
        "_index": util.getIndex(),
        "_type": "mail",
        "_source": {
            'mailID': ID,
            'normalized_date_notime': normalized_date_notime,
        }

    }
    actions.append(action)
    ################################################################################

    #################################### Manage FROM field ##########################

    # Original field, something like: Valerio Albini <valerio.albini01@gmail.com>
    from_str = message.get('from')
    from_tuple = email.utils.parseaddr(from_str)
    from_name = from_tuple[0]
    from_address = from_tuple[1]

    from_name = unicode(from_name,'utf8',errors='replace')

    action = {
        "_index": util.getIndex(),
        "_type": "mail",
        "_source": {
            'mailID': ID,
            'from_name': from_name,
        }

    }
    actions.append(action)

    from_address = unicode(from_address,'utf8',errors='replace')
    action = {
        "_index": util.getIndex(),
        "_type": "mail",
        "_source": {
            'mailID': ID,
            'from_address': from_address,
        }

    }
    actions.append(action)

    #################################################################################


    dbmanager.bulk(actions)
    return ID


def parseMailAttachment(ID, PATH_NAME, part, path):

    actions = []

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

        body = unicode(part.get_payload(decode='True'),'utf8',errors='replace')
        try:
            lang = detect(body)
        except:
            lang = 'unknown'

        try:
            charset = chardet.detect(part.get_payload(decode='True'))
        except:
            charset = 'unknown'
        charset_part = part.get_content_charset()

        action = {
            "_index": util.getIndex(),
            "_type": "mails",
            "_source": {
                'mailID': ID,
                'text/plain': body,
                'lang': lang,
                'charset_header' : charset_part,
                'charset_detected': charset,
            }
        }

        actions.append(action)

        dbmanager.bulk(actions)

    elif ftype == 'text/html':

        body = unicode(part.get_payload(decode='True'),'utf8',errors='replace')

        action = {
            "_index": util.getIndex(),
            "_type": "mails",
            "_source": {
                'mailID': ID,
                'text/html': body,
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
                        "_index": util.getIndex(),
                        "_type": "mail",
                        "_source": {
                             'filePath': unicode(filePath,'utf8',errors='replace'),
                             unicode(output[0].strip(" ").replace(".", "_"),'utf8',errors='replace'): unicode(output[1].strip(" "),'utf8', errors='replace'),
                         }

                     }

                    actions.append(action)

            dbmanager.bulk(actions)
        except:

            actions = []
            print 'Problem with file-parsing in a mail'

            filePath = path + '/' + str(ID) + '/' + str(name)

            action = {
                "_index": util.getIndex(),
                "_type": "mail",
                "_source": {
                    'mailID': ID,
                    "exception": unicode("Problem with file parsing: " + filePath,'utf8',errors='replace'),
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
            "_index": util.getIndex(),
            "_type": "file-system-metadata",
            "_source": {
                "filePath": unicode(path,'utf8',errors='replace'),
                key: value,
            }

        }

        actions.append(action)

    dbmanager.bulk(actions)

def convert_pst_to_mbox(pstfilename, outputfolder):
    print 'Starting conversion into folder '+outputfolder+ ' of the PST: '+pstfilename
    subprocess.call(['readpst', '-o', outputfolder, '-D', '-q', pstfilename])
    print 'Conversion done'
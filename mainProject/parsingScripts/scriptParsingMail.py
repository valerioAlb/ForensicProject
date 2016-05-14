#application/mbox message/rfc822
from email.parser import Parser
import mailbox
import rarfile
import tempfile
import zipfile
import io
import importlib
import subprocess
import sys
sys.path.insert(0, '../')

dbManager = importlib.import_module("dbManager")
dbmanager = dbManager.Manager()
#es = elasticsearch.Elasticsearch("127.0.0.1:9200")

def fileParse(PATH_NAME,extension):

    if extension == 'message/rfc822':
        message = Parser().parse(open(PATH_NAME, 'r'))
        ID = message['Message-ID']
        for x in message.items():
            doc = {
                'mailID': ID,
                x[0].replace(".",""): x[1],
            }
            dbmanager.push('forensic_db','mails',doc)
            #es.index(index='forensic_db', doc_type='mails', body=doc)

        for part in message.walk():
            parseMailAttachment(ID, PATH_NAME, part)

    # Here parsing for mailboxes
    if extension == 'application/mbox':
        mbox = mailbox.mbox(PATH_NAME)
        numMail = len(mbox)
        j = 1
        #print 'parsing mailbox ' + PATH_NAME
        #print 'mailbox ' + PATH_NAME + ' has ' + str(numMail) + ' elements'
        for message in mbox:
            ID = message['Message-ID']

            for x in message.items():
                doc = {
                    'mailID': ID,
                    x[0].replace(".",""): x[1],
                }
                dbmanager.push('forensic_db', 'mails', doc)
                #es.index(index='forensic_db', doc_type='mails', body=doc)

            #From now on we consider the mail attachments

            for part in message.walk():
                parseMailAttachment(ID,PATH_NAME,part)

            if (j % 100 == 0):
                print('Parsed ' + str(j) + " mails of " + str(numMail) + ' for mailbox ' + PATH_NAME)
            j = j + 1

    #print 'Mails parsed.'
    return 0


def parseMailAttachment(ID, PATH_NAME, part):
    name = part.get_filename()

    ftype = part.get_content_type()

    if ftype == 'application/zip':
        data = part.get_payload()
        decodedData = data.decode('base64')
        f = io.BytesIO(decodedData)
        archive = zipfile.ZipFile(f)
        for file in archive.infolist():
            #path = os.path.join(PATH_NAME,ID,name,file.filename)
            path = PATH_NAME + '/' + str(ID) + '/' + str(name)+'/'+str(file.filename)
            zipFileName = file.filename
            zipFileSize = file.file_size
            zipCompressSize = file.compress_size
            zipCreateSystem = file.create_system
            zipInternalAttr = file.internal_attr
            zipExternalAttr = file.external_attr
            zipDateTime = file.date_time

            uploadDatabaseZip(path, zipCompressSize, zipCreateSystem, zipDateTime, zipExternalAttr,
                           zipFileName, zipFileSize, zipInternalAttr)

        f.close()
    elif ftype == 'application/rar':
        data = part.get_payload()
        decodedData = data.decode('base64')
        f = io.BytesIO(decodedData)
        archive = rarfile.RarFile(f)
        for file in archive.infolist():
            #path = os.path.join(PATH_NAME, ID, name, file.filename)
            path = PATH_NAME + '/' + str(ID) + '/' + str(name) + '/' + str(file.filename)
            rarFileName = file.filename
            rarFileSize = file.file_size
            rarCompressSize = file.compress_size
            rarDateTime = file.date_time

            uploadDatabaseRar(path, rarCompressSize, rarDateTime, rarFileName, rarFileSize)
        f.close()

    elif ftype == 'text/plain':
        path = PATH_NAME + '/' + str(ID)
        body = part.get_payload()  # to control automatic email-style MIME decoding (e.g., Base64, uuencode, quoted-printable)
        doc = {
            'filePath': path,
            'text/plain': body,
        }
        try:
            dbmanager.push('forensic_db', 'mails', doc)
            #es.index(index='forensic_db', doc_type='mails', body=doc)
        except:
            #print 'error with Mail plain: '
            #print ID
            temp = unicode(body, errors='ignore')
            doc = {
                'mailID': ID,
                'payload': temp,
            }
            dbmanager.push('forensic_db', 'mails', doc)
            #es.index(index='forensic_db', doc_type='mails', body=doc)

    elif ftype == 'text/html':
        path = PATH_NAME + '/' + str(ID)
        body = part.get_payload()  # to control automatic email-style MIME decoding (e.g., Base64, uuencode, quoted-printable)
        doc = {
            'filePath': path,
            'text/html': body,
        }
        try:
            dbmanager.push('forensic_db', 'mails', doc)
            #es.index(index='forensic_db', doc_type='mails', body=doc)
        except:
            #print 'error with Mail html: '
            #print ID
            temp = unicode(body, errors='ignore')
            doc = {
                'mailID': ID,
                'payload': temp,
            }
            dbmanager.push('forensic_db', 'mails', doc)
            #es.index(index='forensic_db', doc_type='mails', body=doc)


    elif 'multipart/' in ftype:
        #nothing to do. The content is duplicated
        pass
    else:
        try:
            path = PATH_NAME + '/' + str(ID) + '/' + str(name)
            data = part.get_payload()
            decodedData = data.decode('base64')
            ###
            temp = tempfile.NamedTemporaryFile()
            temp.write(decodedData)
            temp.seek(0)
            p1 = subprocess.Popen(["exiftool", temp.name], stdout=subprocess.PIPE)
            result = p1.communicate()[0]
            tokens = result.split('\n')
            # print tokens
            #print 'File metadata---------------------------------------------'
            for token in tokens:
                if token != '':
                    output = token.split(':', 1)
                    doc = {
                        "filePath": path,
                        output[0].strip(" ").replace(".",""): output[1].strip(" "),
                    }
                    dbmanager.push('forensic_db', 'file-metadata', doc)
        except:
            path = PATH_NAME + '/' + str(ID) + '/' + str(name)
            doc = {
                "filePath": path,
                "exception": "Problem with file parsing",
            }
            dbmanager.push('forensic_db', 'exception', doc)
        finally:
            # Automatically cleans up the file
            try:
                temp.close()
            except:
                pass


def uploadDatabaseZip(path, compressSize, createSystem, dateTime, externalAttr, fileName, fileSize,
                   internalAttr):
    doc = {
        'filePath': path,
        'size': fileSize,
    }
    dbmanager.push('forensic_db', 'file-system-metadata', doc)
    #es.index(index='forensic_db', doc_type='file-system-metadata', body=doc)
    doc = {
        'filePath': path,
        'compressSize': compressSize,
    }
    dbmanager.push('forensic_db', 'file-system-metadata', doc)
    #es.index(index='forensic_db', doc_type='file-system-metadata', body=doc)
    doc = {
        'filePath': path,
        'createSystem': createSystem,
    }
    dbmanager.push('forensic_db', 'file-system-metadata', doc)
    #es.index(index='forensic_db', doc_type='file-system-metadata', body=doc)
    doc = {
        'filePath': path,
        'fileName': fileName,
    }
    dbmanager.push('forensic_db', 'file-system-metadata', doc)
    #es.index(index='forensic_db', doc_type='file-system-metadata', body=doc)
    doc = {
        'filePath': path,
        'internalAttr': internalAttr,
    }
    dbmanager.push('forensic_db', 'file-system-metadata', doc)
    #es.index(index='forensic_db', doc_type='file-system-metadata', body=doc)
    doc = {
        'filePath': path,
        'externalAttr': externalAttr,
    }
    dbmanager.push('forensic_db', 'file-system-metadata', doc)
    #es.index(index='forensic_db', doc_type='file-system-metadata', body=doc)
    doc = {
        'filePath': path,
        'dateTime': dateTime,
    }
    dbmanager.push('forensic_db', 'file-system-metadata', doc)
    #es.index(index='forensic_db', doc_type='file-system-metadata', body=doc)


def uploadDatabaseRar(path, compressSize, dateTime, fileName, fileSize):

    doc = {
        'filePath': path,
        'size': fileSize,
    }
    dbmanager.push('forensic_db', 'file-system-metadata', doc)
    #es.index(index='forensic_db', doc_type='file-system-metadata', body=doc)
    doc = {
        'filePath': path,
        'compressSize': compressSize,
    }
    dbmanager.push('forensic_db', 'file-system-metadata', doc)
    #es.index(index='forensic_db', doc_type='file-system-metadata', body=doc)

    doc = {
        'filePath': path,
        'fileName': fileName,
    }
    dbmanager.push('forensic_db', 'file-system-metadata', doc)
    #es.index(index='forensic_db', doc_type='file-system-metadata', body=doc)


    doc = {
        'filePath': path,
        'dateTime': dateTime,
    }
    dbmanager.push('forensic_db', 'file-system-metadata', doc)
    #es.index(index='forensic_db', doc_type='file-system-metadata', body=doc)
#application/mbox message/rfc822
from email.parser import Parser
import mailbox
import rarfile
import zipfile
import elasticsearch
import io
import exifread
es = elasticsearch.Elasticsearch("127.0.0.1:9200")

def fileParse(PATH_NAME,extension):

    if extension == 'message/rfc822':
        message = Parser().parse(open(PATH_NAME, 'r'))
        ID = message['Message-ID']
        for x in message.items():
            doc = {
                'mailID': ID,
                x[0]: x[1],
            }
            es.index(index='forensic_db', doc_type='mails', body=doc)

        for part in message.walk():
            parseMailAttachment(ID, PATH_NAME, part)

    # Here parsing for mailboxes
    if extension == 'application/mbox':
        mbox = mailbox.mbox(PATH_NAME)
        numMail = len(mbox)
        j = 1
        print 'parsing mailbox ' + PATH_NAME
        print 'mailbox ' + PATH_NAME + ' has ' + str(numMail) + ' elements'
        for message in mbox:
            ID = message['Message-ID']

            for x in message.items():
                doc = {
                    'mailID': ID,
                    x[0]: x[1],
                }
                es.index(index='forensic_db', doc_type='mails', body=doc)

            #From now on we consider the mail attachments

            for part in message.walk():
                parseMailAttachment(ID,PATH_NAME,part)

            if (j % 100 == 0):
                print('Parsed ' + str(j) + " mails of " + str(numMail) + ' for mailbox ' + PATH_NAME)
            j = j + 1

    print 'Mails parsed.'
    return 0


def parseMailAttachment(ID, PATH_NAME, part):
    name = part.get_filename()

    ftype = part.get_content_type()
    if "image/" in ftype:
        #path = os.path.join(PATH_NAME,ID,name)
        path = PATH_NAME+'/'+str(ID)+'/'+str(name)
        data = part.get_payload()
        decodedData = data.decode('base64')
        f = io.BytesIO(decodedData)
        tags = exifread.process_file(f)
        for tag in tags.keys():
            if tag not in ('JPEGThumbnail', 'TIFFThumbnail', 'EXIF MakerNote'):
                doc = {
                    'filePath': path,
                    tag: tags[tag]
                }
                es.index(index='forensic_db', doc_type='file-metadata', body=doc)
        f.close()
    elif ftype == 'application/zip':
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

            uploadDatabase(path, zipCompressSize, zipCreateSystem, zipDateTime, zipExternalAttr,
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
            rarCreateSystem = file.create_system
            rarInternalAttr = file.internal_attr
            rarExternalAttr = file.external_attr
            rarDateTime = file.date_time

            uploadDatabase(path, rarCompressSize, rarCreateSystem, rarDateTime, rarExternalAttr, rarFileName, rarFileSize,
                       rarInternalAttr)
        f.close()

    elif ftype == 'text/plain':
        path = PATH_NAME + '/' + str(ID)
        body = part.get_payload(decode=True)  # to control automatic email-style MIME decoding (e.g., Base64, uuencode, quoted-printable)
        doc = {
            'filePath': path,
            'text/plain': body,
        }
        try:
            es.index(index='forensic_db', doc_type='mail', body=doc)
        except:
            temp = unicode(body, errors='ignore')
            doc = {
                'mailID': ID,
                'payload': temp,
            }
            es.index(index='forensic_db', doc_type='mails', body=doc)

    elif ftype == 'text/html':
        path = PATH_NAME + '/' + str(ID)
        body = part.get_payload(decode=True)  # to control automatic email-style MIME decoding (e.g., Base64, uuencode, quoted-printable)
        body = body.decode()
        doc = {
            'filePath': path,
            'text/html': body,
        }
        try:
            es.index(index='forensic_db', doc_type='mail', body=doc)
        except:
            temp = unicode(body, errors='ignore')
            doc = {
                'mailID': ID,
                'payload': temp,
            }
            es.index(index='forensic_db', doc_type='mails', body=doc)


    elif 'multipart/' in ftype:
        print
    else:
        #path = os.path.join(PATH_NAME,ID,name)
        path = PATH_NAME + '/' + str(ID) + '/' + str(name)
        doc = {
            'filePath': path,
            'exeption': 'Unable to parse'
        }
        es.index(index='forensic_db', doc_type='exeption', body=doc)


def uploadDatabase(path, compressSize, createSystem, dateTime, externalAttr, fileName, fileSize,
                   internalAttr):
    doc = {
        'filePath': path,
        'size': fileSize,
    }
    es.index(index='forensic_db', doc_type='file-system-metadata', body=doc)
    doc = {
        'filePath': path,
        'compressSize': compressSize,
    }
    es.index(index='forensic_db', doc_type='file-system-metadata', body=doc)
    doc = {
        'filePath': path,
        'createSystem': createSystem,
    }
    es.index(index='forensic_db', doc_type='file-system-metadata', body=doc)
    doc = {
        'filePath': path,
        'fileName': fileName,
    }
    es.index(index='forensic_db', doc_type='file-system-metadata', body=doc)
    doc = {
        'filePath': path,
        'internalAttr': internalAttr,
    }
    es.index(index='forensic_db', doc_type='file-system-metadata', body=doc)
    doc = {
        'filePath': path,
        'externalAttr': externalAttr,
    }
    es.index(index='forensic_db', doc_type='file-system-metadata', body=doc)
    doc = {
        'filePath': path,
        'dateTime': dateTime,
    }
    es.index(index='forensic_db', doc_type='file-system-metadata', body=doc)
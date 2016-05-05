#application/mbox message/rfc822
from email.parser import Parser
import mailbox
import rarfile
import zipfile
import base64
from StringIO import StringIO
#ucomment everything to put on elasticsearch
#es = elasticsearch.Elasticsearch("127.0.0.1:9200")


#extension = mimetypes.MimeTypes().guess_type(PATH_NAME)[0]

def fileParse(PATH_NAME,extension):
    i = 1
    if extension == 'message/rfc822':
        print PATH_NAME
        message = Parser().parse(open(PATH_NAME, 'r'))
        ID = message['Message-ID']
        print ID
        for x in message.items():
            doc = {
                'mailID': ID,
                x[0]: x[1],
            }
        #es.index(index='forensic_db', doc_type='mails', body=doc)

        for part in message.walk():
            if part.get_content_type() == 'application/zip':
                zip_bytes = base64.b64decode(part.get_payload())
                file_wrapper = StringIO(zip_bytes)
                if zipfile.is_zipfile(file_wrapper):
                    zf = zipfile.ZipFile(file_wrapper, 'r')
                    files = zf.infolist()
                    for info in files:
                        print '**********************************************'
                        fname = info.filename
                        # data = zf.read(fname)
                        print fname
                        # print data

            elif part.get_content_type() == 'application/rar':
                rar_bytes = base64.b64decode(part.get_payload())
                file_wrapper = StringIO(rar_bytes)
                rf = rarfile.RarFile(file_wrapper)
                files = rf.infolist()
                for info in files:
                    print '**********************************************'
                    fname = info.filename
                    # data = zf.read(fname)
                    print fname
                    # print data
            else:
                print part.get_content_type()

        try:
            i=i
            #es.index(index='forensic_db', doc_type='mails', body=doc)
        except:
            print 'error with Mail: '
            print ID

            #es.index(index='forensic_db', doc_type='mails', body=doc)

        if (i % 100 == 0):
            print('Parsed ' + str(i) + " mails")
        i = i + 1

    # Here parsing for mailboxes
    if extension == 'application/mbox':
        mbox = mailbox.mbox(PATH_NAME)
        numMail = len(mbox)
        j = 1
        print 'parsing mailbox ' + PATH_NAME
        print 'mailbox ' + PATH_NAME + ' has ' + str(numMail) + ' elements'
        for message in mbox:

            ID = message['Message-ID']
            print ID

            for x in message.items():
                doc = {
                    'mailID': ID,
                    x[0]: x[1],
                }
                #es.index(index='forensic_db', doc_type='mails', body=doc)

            #From now on we consider the mail attachments

            for part in message.walk():
                if part.get_content_type() == 'application/zip':
                    print part.get_content_type()
                    zip_bytes = base64.b64decode(part.get_payload())
                    file_wrapper = StringIO(zip_bytes)
                    if zipfile.is_zipfile(file_wrapper):
                        zf = zipfile.ZipFile(file_wrapper, 'r')
                        files = zf.infolist()
                        for info in files:
                            print '**********************************************'
                            fname = info.filename
                            # data = zf.read(fname)
                            print fname
                            # print data

                elif part.get_content_type() == 'application/rar':
                    print part.get_content_type()
                    rar_bytes = base64.b64decode(part.get_payload())
                    file_wrapper = StringIO(rar_bytes)
                    rf = rarfile.RarFile(file_wrapper)
                    files = rf.infolist()
                    for info in files:
                        print '**********************************************'
                        fname = info.filename
                        # data = zf.read(fname)
                        print fname
                        # print data
                else:
                    print part.get_content_type()
                    if part.get_filename() != None:
                        print part.get_filename()

            try:
                i = i
                #es.index(index='forensic_db', doc_type='mails', body=doc)
            except:
                print 'error with Mail: '
                print ID

                #es.index(index='forensic_db', doc_type='mails', body=doc)

            if (j % 100 == 0):
                print('Parsed ' + str(j) + " mails of " + str(numMail) + 'for mailbox ' + PATH_NAME)
            j = j + 1

        if (i % 100 == 0):
            print('Parsed ' + str(i) + " mails")
        i = i + 1

    print 'Mails parsed.'
    return 0
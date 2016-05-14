import walkerClass
import fileParserClass
import logging
import os
import dbManager

LOG_PATH = './LOG.log'

walkpath='/home/valerio/Documenti/Forensic/TEST FILE METADATA/'

if __name__ == '__main__':

    logging.basicConfig(filename='./LOG.log',level=logging.INFO,format=' [%(levelname)s] %(asctime)s %(message)s')

    parser = fileParserClass.FileParser()
    parser.printMimeSupported()

    dbmanager = dbManager.Manager()

    breackpointFile = '###'

    if os.path.isfile(LOG_PATH):
        file = open(LOG_PATH, 'rb')
        for line in file:
            if line.split(' ')[1] == '[INFO]':
                if line.split(' ')[4] == '[BREAKPOINT]':
                    breackpointFile = line.split('#')[-1].rstrip('\n')

                # If the log file ends with [END] tag, it means that in the previous session there was no crash
                elif line.split(' ')[4] == '[END]':
                    breackpointFile = '###'

    print breackpointFile
    logging.info('[START] Program Started')

    methodClass = walkerClass.Walker(parser,breackpointFile)
    methodClass.WalkPath(walkpath)

    print 'Program succesfully ended'

    logging.info('[END] Program Ended')

    def pushToDatabase(index,doc_type,body):

        dbmanager.push(index,doc_type,body)
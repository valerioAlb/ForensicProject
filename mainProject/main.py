import walkerClass
import fileParserClass
import logging
import os

LOG_PATH = './LOG.log'

if __name__ == '__main__':

    logging.basicConfig(filename='./LOG.log',level=logging.DEBUG,format=' [%(levelname)s] %(asctime)s %(message)s')
    logging.info('Program Started')
    parser = fileParserClass.FileParser()
    parser.printMimeSupported()

    if os.path.isfile(LOG_PATH):
        breackpointFile = '###'
        file = open(LOG_PATH, 'rb')
    else:
        breackpointFile = '###'

    methodClass = walkerClass.Walker(parser,breackpointFile)
    methodClass.WalkPath('/home/valerio/Documenti/Forensic/TEST FILE METADATA')

    logging.info('Program Ended')
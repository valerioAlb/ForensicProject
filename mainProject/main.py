import walkerClass
import fileParserClass
import logging
import os
import dbManager
from time import sleep

LOG_PATH = './LOG.log'

walkpath='/home/valerio/Documenti/Forensic/TEST'

if __name__ == '__main__':

    logging.basicConfig(filename='./LOG.log',level=logging.INFO,format=' [%(levelname)s] %(asctime)s %(message)s')
    #logging.getLogger('elasticsearch').addHandler(logging.NullHandler())

    ###################################################################
    es_logger = logging.getLogger('elasticsearch')
    es_logger.propagate = False
    es_logger.setLevel(logging.INFO)
    es_logger_handler = logging.NullHandler()
    es_logger.addHandler(es_logger_handler)

    es_tracer = logging.getLogger('elasticsearch.trace')
    es_tracer.propagate = False
    es_tracer.setLevel(logging.DEBUG)
    es_tracer_handler =  logging.NullHandler()
    es_tracer.addHandler(es_tracer_handler)

    logger = logging.getLogger('mainLog')
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    # create file handler
    fileHandler =  logging.NullHandler()
    fileHandler.setLevel(logging.INFO)
    # create console handler
    consoleHandler = logging.StreamHandler()
    consoleHandler.setLevel(logging.INFO)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    consoleHandler.setFormatter(formatter)
    fileHandler.setFormatter(formatter)
    # add the handlers to logger
    logger.addHandler(consoleHandler)
    logger.addHandler(fileHandler)
    ####################################################################

    parser = fileParserClass.FileParser()
    parser.printMimeSupported()
    dbmanager = dbManager.dbManager.get_instance()

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

    print 'Breackpoint founded: ',breackpointFile
    sleep(3)
    print 'Program Started'
    logging.info('[START] Program Started')

    methodClass = walkerClass.Walker(parser,breackpointFile)
    methodClass.WalkPath(walkpath)

    print 'Program succesfully ended'

    logging.info('[END] Program Ended')

    def pushToDatabase(index,doc_type,body):

        dbmanager.push(index,doc_type,body)

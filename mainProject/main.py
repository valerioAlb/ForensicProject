import walkerClass
import fileParserClass
import logging
import os
import dbManager
import utils
from time import sleep

LOG_PATH = './LOG.log'

walkpath='/media/valerio/HD2/Backup/Download'

if __name__ == '__main__':

    logging.basicConfig(filename='./LOG.log',level=logging.INFO,format=' [%(levelname)s] %(asctime)s %(message)s')

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

    # Initialize the dbManager that will be used in the program
    dbmanager = dbManager.dbManager.get_instance()

    util = utils.utils.get_instance()
    # By default the recursive level on archives is 1
    util.setMaxRecursionLevel(1)

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

    # Pause before continuing with the program
    sleep(3)
    print 'Program Started'
    logging.info('[START] Program Started')

    methodClass = walkerClass.Walker(parser,breackpointFile)
    methodClass.WalkPath(walkpath)

    dbmanager.forceBulk()

    print 'Program succesfully ended'

    logging.info('[END] Program Ended')


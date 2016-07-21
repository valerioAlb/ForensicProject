import walkerClass
import fileParserClass
import logging
import time
import dbManager
import utils
import os.path
from time import sleep

LOG_PATH = './LOG.log'

walkpath='/home/valerio/Scrivania/DatasetMBOX'
#walkpath='/home/valerio/Scrivania/Mail di Test'

# walkpath='/media/valerio/ExternalHD/RevisedEDRMv1_Complete/RevisedEDRMv1_Complete/andrew_lewis/'

if __name__ == '__main__':

    logging.basicConfig(filename='./LOG.log',level=logging.INFO,format=' [%(levelname)s] %(asctime)s %(message)s')

    # Find the total number of files, useful to show the progress
    cpt = sum([len(files) for r, d, files in os.walk(walkpath)])
    print 'Total number of files ', cpt
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

    # Print the support fo type of file
    parser = fileParserClass.FileParser()
    parser.printMimeSupported()

    util = utils.utils.get_instance()
    util.setIndex('forensic_db_four')
    print '############: ', util.getIndex()

    # Initialize the dbManager that will be used in the program
    dbmanager = dbManager.dbManager.get_instance()
    dbmanager.initializeDB()


    # Clear, all temp folders by unmounting, deleting ..

    print 'SettinUp environment ....'
    util.setUpEnvironment()
    util.setNumbFiles(cpt)

    # By default the recursive level on archives is 1
    util.setMaxRecursionLevel(2)


    print 'Done ...'

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
    timeStart = (time.strftime("%H:%M:%S")) + ' ' + (time.strftime("%d/%m/%Y"))
    print 'Program Started'
    logging.info('[START] Program Started')

    methodClass = walkerClass.Walker(parser,breackpointFile)
    methodClass.WalkPath(walkpath)

    dbmanager.forceBulk()
    timeEnd = (time.strftime("%H:%M:%S")) + ' ' + (time.strftime("%d/%m/%Y"))
    print 'Program succesfully ended'

    logging.info('Start: '+timeStart+' End: '+timeEnd)
    logging.info('[END] Program Ended')


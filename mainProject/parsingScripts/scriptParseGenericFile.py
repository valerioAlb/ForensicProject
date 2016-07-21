#file/generic
import subprocess
import importlib
import sys
sys.path.insert(0, '../')


def fileParse(PATH_NAME,mime,realPath=""):

    if realPath == "":
        path = PATH_NAME
    else:
        path = realPath

    dbManager = importlib.import_module("dbManager")
    dbmanager = dbManager.dbManager.get_instance()

    utils = importlib.import_module("utils")
    util = utils.utils.get_instance()

    p1 = subprocess.Popen(["exiftool", PATH_NAME], stdout=subprocess.PIPE)
    result = p1.communicate()[0]
    # Now process the result, getting the lines with values
    tokens = result.split('\n')

    actions = []

    for token in tokens:
        if token != '':
            output = token.split(':', 1)
            action = {
                "_index": util.getIndex(),
                "_type": "file-metadata",
                "_source": {
                    "filePath": unicode(path,'utf8',errors='replace'),
                    unicode(output[0].strip(" ").replace(".", "_"),'utf8',errors='ignore'): unicode(output[1].strip(" "),'utf8',errors='ignore')
                }

            }

            actions.append(action)

    dbmanager.bulk(actions)

    return 0
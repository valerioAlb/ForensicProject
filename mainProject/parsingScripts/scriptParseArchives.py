#application/zip application/x-compressed-tar application/x-rar
import subprocess
import sys
import importlib
import scriptParseGenericFile
import os
sys.path.insert(0, '../')

utils = importlib.import_module("utils")
util = utils.utils.get_instance()

def fileParse(PATH_NAME,extension, realPath=""):

    if realPath == "":
        realPath = PATH_NAME

    scriptParseGenericFile.fileParse(PATH_NAME,extension, realPath)

    a = util.setRecursion()

    if a == 0:
        print 'Stop recursion'
        return

    base = os.path.basename(PATH_NAME)

    if not os.path.exists(util.getTempDirArchives()):
        os.mkdir(util.getTempDirArchives())

    dirTemp = util.getTempDirArchives()+base

    parser = importlib.import_module("fileParserClass")
    walker = importlib.import_module("walkerClass")

    # Mount the file
    p1 = subprocess.Popen(["mkdir",dirTemp],stdout=subprocess.PIPE)
    p1.communicate()
    print 'Mounting the archive : '+ realPath

    p1 = subprocess.Popen(["archivemount","-o","readonly",PATH_NAME, dirTemp], stdout=subprocess.PIPE)
    p1.communicate()
    print 'Archive mounted.'

    parser = parser.FileParser()
    methodClass = walker.Walker(parser,'###')

    methodClass.WalkPath(dirTemp, realPath)


    # In the end umount the file
    p1 = subprocess.Popen(["umount",dirTemp], stdout=subprocess.PIPE)
    p1.communicate()
    p1 = subprocess.Popen(["rmdir",dirTemp], stdout=subprocess.PIPE)
    p1.communicate()

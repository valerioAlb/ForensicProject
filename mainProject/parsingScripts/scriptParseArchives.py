#application/zip
import subprocess
import sys
import importlib
import os
sys.path.insert(0, '../')

def fileParse(PATH_NAME,extension):

    base = os.path.basename(PATH_NAME)
    dirTemp = "/home/valerio/temp/"+base

    parser = importlib.import_module("fileParserClass")
    walker = importlib.import_module("walkerClass")

    # Mount the file
    p1 = subprocess.Popen(["mkdir",dirTemp],stdout=subprocess.PIPE)
    p1.communicate()
    p1 = subprocess.Popen(["archivemount","-o","readonly",PATH_NAME, dirTemp], stdout=subprocess.PIPE)
    p1.communicate()

    parser = parser.FileParser()
    methodClass = walker.Walker(parser,'###')
    methodClass.WalkPath(dirTemp)


    #Umount the file
    p1 = subprocess.Popen(["umount",dirTemp], stdout=subprocess.PIPE)
    p1.communicate()
    p1 = subprocess.Popen(["rmdir",dirTemp], stdout=subprocess.PIPE)
    p1.communicate()

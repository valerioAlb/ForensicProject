import subprocess
import os
#useful to manage different aspect of the application.
class utils(object):

    INSTANCE = None

    maxRecursionLevel = 1
    actualRecursionLevel = 0

    dim_counter = 0
    BREAKPOINT_DIMENSION = 100


    tempDirArchives = "/home/valerio/temp/"



    def __init__(self):
        if self.INSTANCE is not None:
            raise ValueError("An instantiation already exist!")

    @classmethod
    def get_instance(cls):
        if cls.INSTANCE is None:
            cls.INSTANCE = utils()
        return cls.INSTANCE

    # This function is useful when considering archives. Whenever entering an archive you check the recursion level
    # reached. If you have exceeded the maximum level you stop
    def setRecursion(self):

        self.actualRecursionLevel = self.actualRecursionLevel + 1
        if self.actualRecursionLevel > self.maxRecursionLevel:
            self.actualRecursionLevel = 0
            return 0
        else:
            return 1

    # To specify a different maxRecursionLevel
    def setMaxRecursionLevel(self,level):
        self.maxRecursionLevel = level

    # Method used to check if a breakpoint should be placed. A breakpoint should be placed before a cumulative
    # dimension of BREAKPOINT_DIMENSION
    def checkBreakPoint(self,dimNextFileToParse):
        if self.dim_counter + dimNextFileToParse > self.BREAKPOINT_DIMENSION * 1000000:
            self.dim_counter = dimNextFileToParse
            return 0
        else:
            self.dim_counter = self.dim_counter + dimNextFileToParse
            return 1

    def setBreakpointDimensionMB(self,dimensionMB):
        self.BREAKPOINT_DIMENSION = dimensionMB

    def setTempDirArchives(self,tempDIr):
        self.tempDirArchives = tempDIr

    def getTempDirArchives(self):
        return self.tempDirArchives

    # Used at the beginning of the program to setup the environment
    def setUpEnvironment(self):
        if os.path.exists(self.tempDirArchives):
            subprocess.call('umount -l ' + self.tempDirArchives + '*', shell=True)
            #shutil.rmtree(self.tempDirArchives)
            subprocess.call('rm -R '+ self.tempDirArchives+ '*', shell=True)
        else:
            os.mkdir(self.tempDirArchives)

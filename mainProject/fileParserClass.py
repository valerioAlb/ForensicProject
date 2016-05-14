import os
import sys
import importlib
#Useful to set the path where all the scripts are located
sys.path.insert(0, './parsingScripts')


scriptFolderPath='parsingScripts/'

class FileParser:

    # The dict variable contains all the couples "mime-type / script able to parse it"
    dict = {}

    #You have to put all the script in the specified folder "scriptFolderPath"
    #The first line should be a comment with the list of all mime-types supported
    #Example: #application/mbox message/rfc
    def __init__(self):
        self.initialization()

    def initialization(self):
            for root, dirs, files in os.walk(scriptFolderPath):
                for file in files:
                    fname = os.path.join(root, file)
                    if os.path.isfile(fname) and str(os.path.splitext(fname)[1])=='.py':
                        with open(fname, 'r') as f:
                            first_line = f.readline().strip()
                            elements = first_line.split(' ')
                            elements[0]=elements[0].lstrip('#')
                            for element in elements:
                                self.dict[element] = os.path.splitext(file)[0];

    def printMimeSupported(self):
        for i in self.dict:
            print '"'+i+'" parsed with the script: "'+ self.dict[i]+'"'

    def parse(self,mime,fname):

        if mime in self.dict.keys():
            # dynamically import only the interested script and use it!
            lib = importlib.import_module(self.dict[mime])
            lib.fileParse(fname,mime)

        else:
            #print "Use generic Parser"
            lib = importlib.import_module(self.dict['file/generic'])
            lib.fileParse(fname, mime)
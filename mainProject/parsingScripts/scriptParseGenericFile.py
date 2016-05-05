#file/generic
import subprocess

def fileParse(PATH_NAME,mime):
    p1 = subprocess.Popen(["exiftool", PATH_NAME], stdout=subprocess.PIPE)
    result = p1.communicate()[0]
    # Now process the result, getting the lines with values
    tokens = result.split('\n')
    # print tokens
    print 'File metadata---------------------------------------------'
    for token in tokens:
        if token != '':
            output = token.split(':', 1)
            print output[0].strip(" ")
            print output[1].strip(" ")

    return 0;
import sys
import time

import os

# precode_dock = '/Users/murftech/Root/Datarepo/'
precode_dock = ''

def showtime():
    # Get the current time in seconds since the epoch
    current_time_seconds = time.time()

    # Convert seconds to a time tuple
    time_tuple = time.localtime(current_time_seconds)

    # Format the time tuple as "HH:MM:SS"
    time_str = time.strftime("%H:%M:%S", time_tuple)
    print(time_str)
    return time_str

def runpy(file_path):
    print('SCRIPT RUN STARTED!!!!!!: ' + file_path)
    print('script started time: ' + showtime())
    precode_path = precode_dock + file_path
    print(precode_path)
    exec(open(precode_path).read(), globals())
    print('SCRIPT HAS FINISHED!!!!!!: ' + file_path)
    print('script finished time: ' + showtime())
    

# def precodepull(file_path):
#     cdsw_path = f'/home/cdsw/{file_path}'
#     exec(open(cdsw_path).read(), globals())
#     print(cdsw_path + 'precode loaded')


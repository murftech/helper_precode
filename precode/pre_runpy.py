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

import logging
import time

import json
from datetime import datetime
from pathlib import Path


JSONL_PATH = Path("/Users/murftech/Library/CloudStorage/OneDrive-Personal/DBMaster/annotations/reports/runlogging/runpy.jsonl")

import builtins


def log_run_record(file_path, status, elapsed, error=None):
    record = {
        "ts": datetime.utcnow().isoformat(),
        "file": file_path,
        "status": status,           # "ok" | "fail"
        "elapsed_sec": builtins.round(elapsed, 3),
        "error": error,
    }
    with JSONL_PATH.open("a") as f:
        f.write(json.dumps(record) + "\n")
        
# def log_run_record(file_path, status, elapsed, error=None):
#     record = {
#         "timestamp": datetime.now().isoformat(timespec="seconds"),
#         "file": file_path,
#         "status": status,
#         "elapsed_sec": round(elapsed, 2),
#         "error": error,
#     }
#     with JSONL_PATH.open("a") as f:
#         f.write(json.dumps(record) + "\n")


# Set up once, at module top
logger = logging.getLogger("runpy")
logger.setLevel(logging.INFO)

fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

console = logging.StreamHandler()
console.setFormatter(fmt)
logger.addHandler(console)

# file_handler = logging.FileHandler("runpy.log")   # persistent history
file_handler = logging.FileHandler('/Users/murftech/Library/CloudStorage/OneDrive-Personal/DBMaster/annotations/reports/runlogging/runpy.log')   # persistent history


# the key is this!

file_handler.setFormatter(fmt)
logger.addHandler(file_handler)


def runpy(file_path):

    precode_path = precode_dock + file_path

    # print('SCRIPT RUN STARTED!!!!!!: ' + file_path)
    # print('script started time: ' + showtime())

    logger.info(f"START  {file_path}  ({precode_path})")
    t0 = time.perf_counter()

    try:
        exec(open(precode_path).read(), globals())
    except Exception:
        elapsed = time.perf_counter() - t0
        logger.exception(f"FAIL   {file_path}  after {elapsed:.2f}s")
        log_run_record(file_path, "fail", elapsed, error=repr(e))

        raise Exception('script failed!')
    else:
        elapsed = time.perf_counter() - t0
        logger.info(f"FINISH {file_path}  in {elapsed:.2f}s")
        log_run_record(file_path, "ok", elapsed)




# no logger run
# def runpy(file_path):

#     print('SCRIPT RUN STARTED!!!!!!: ' + file_path)
#     print('script started time: ' + showtime())
#     precode_path = precode_dock + file_path
#     print(precode_path)
#     exec(open(precode_path).read(), globals())
#     print('SCRIPT HAS FINISHED!!!!!!: ' + file_path)
#     print('script finished time: ' + showtime())
    

# def precodepull(file_path):
#     cdsw_path = f'/home/cdsw/{file_path}'
#     exec(open(cdsw_path).read(), globals())
#     print(cdsw_path + 'precode loaded')


# def runr(arg_rscript):

#     import subprocess

#     process = subprocess.Popen(
#         ['Rscript', arg_rscript], 
#         stdout=subprocess.PIPE, 
#         stderr=subprocess.STDOUT, # Redirect errors to the same stream
#         text=True,
#         bufsize=1 # Line buffered
#     )

#     # Loop over the output as it arrives
#     for line in process.stdout:
#         print(f"R Output: {line.strip()}")

#     # Wait for the process to actually finish
#     process.wait()

#     print('SCRIPT HAS FINISHED!!!!!!: ' + arg_rscript)
#     print('script finished time: ' + showtime())



# def runr(arg_rscript):

#     import subprocess

#     logger.info(f"START  {arg_rscript} ")
#     t0 = time.perf_counter()

#     try:
#         process = subprocess.Popen(
#             ['Rscript', arg_rscript], 
#             stdout=subprocess.PIPE, 
#             stderr=subprocess.STDOUT, # Redirect errors to the same stream
#             text=True,
#             bufsize=1 # Line buffered
#         )

#         # Loop over the output as it arrives
#         for line in process.stdout:
#             print(f"R Output: {line.strip()}")

#         # Wait for the process to actually finish
#         process.wait()  
          
#     except Exception:
#         elapsed = time.perf_counter() - t0
#         logger.exception(f"FAIL   {arg_rscript}  after {elapsed:.2f}s")
#         log_run_record(arg_rscript, "fail", elapsed, error=repr(e))

#         raise
#     else:
#         elapsed = time.perf_counter() - t0
#         logger.info(f"FINISH {arg_rscript}  in {elapsed:.2f}s")
#         log_run_record(arg_rscript, "ok", elapsed)



def runr(arg_rscript):
    import subprocess

    logger.info(f"START  {arg_rscript} ")
    t0 = time.perf_counter()

    try:
        process = subprocess.Popen(
            ['Rscript', arg_rscript],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,   # merge stderr into stdout
            text=True,
            bufsize=1                   # line buffered
        )

        # Stream output as it arrives
        for line in process.stdout:
            print(f"R Output: {line.rstrip()}")

        # Wait for the process to actually finish
        process.wait()

        # *** THE FIX: turn a non-zero exit into a Python exception ***
        if process.returncode != 0:
            raise subprocess.CalledProcessError(
                process.returncode,
                ['Rscript', arg_rscript],
            )

    except Exception as e:
        elapsed = time.perf_counter() - t0
        logger.exception(f"FAIL   {arg_rscript}  after {elapsed:.2f}s")
        log_run_record(arg_rscript, "fail", elapsed, error=repr(e))
        raise   # re-raise so the outer driver halts
    else:
        elapsed = time.perf_counter() - t0
        logger.info(f"FINISH {arg_rscript}  in {elapsed:.2f}s")
        log_run_record(arg_rscript, "ok", elapsed)
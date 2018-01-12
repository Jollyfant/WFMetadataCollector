"""

  EIDA WFMetadataCollector

  Script for ingestion of waveform metadata from an SDS archive including:
    - Digital Objects
    - Waveform Metadata
    - Waveform Spectra

  Author: Mathijs Koymans, 2018
  Copyright: ORFEUS Data Center
  __VERSION__: 2.0.0

"""

from __future__ import division, print_function

# System libs
import time
import json
import logging
import sys
import os
import argparse
import signal

# Libs
from datetime import datetime
from multiprocessing import Pool, TimeoutError

# Custom libs
from wfcatalog import WFMetadataCollector
from mongo import WFCatalogDB
from filestream import SDSFileFinder
from config import CONFIG

def ParseArguments():

  # Parse cmd line arguments
  parser = argparse.ArgumentParser(description="Process seismic waveform metadata to Mongo repositories.")

  PAST_OPTIONS = [
    "today",
    "yesterday",
    "week",
    "fortnight",
    "month"
  ]

  # File input options
  parser.add_argument("--dir", help="Directory containing the files to process (works recursively).", default=None)
  parser.add_argument("--file", help="Specific file to be processed.", default=None)
  parser.add_argument("--glob", help="Glob expression for SDS filename to be processed.", default=None)
  parser.add_argument("--list", help="Specific JSON formatted list of files to be processed.", default=None)
  parser.add_argument("--past", help="Process files in a specific preset range in the past.", choices=PAST_OPTIONS, default=None)
  parser.add_argument("--date", help="Process files for a specific date.", default=None)
  parser.add_argument("--range", help="Process until number of days after (positive) or before (negative) a specific date.", type=int, default=1)

  # Options to show config/versioning
  parser.add_argument("--config", help="View configuration options", action="store_true")
  parser.add_argument("--version", help="View configuration options", action="store_true")
  parser.add_argument("--debug", help="View configuration options", action="store_true")

  # Set custom logfile
  parser.add_argument("--logfile", help="Write output to logfile.", default=None)

  # Force updates without checksum change
  parser.add_argument("--force", help="Force file updates.", action="store_true")

  # Choice to store metrics and PSDs
  parser.add_argument("--store-psd", help="Add the spectral densities to the database.", action="store_true")
  parser.add_argument("--store-metrics", help="Add the metrics to the database.", action="store_true")

  return vars(parser.parse_args())


if __name__ == "__main__":

  """
  Main function for multiprocessing the collection
  of waveform metadata
  """

  initialized = datetime.now()

  # Get the command line arguments as a dict
  args = ParseArguments()

  # Print the application version
  if args["version"]:
    print(CONFIG["__VERSION__"]); sys.exit(0)

  # Print config and abandon ship
  if args["config"]:
    print(json.dumps(CONFIG, indent=4)); sys.exit(0)

  # Set up the logger
  logging.basicConfig(
    level=logging.DEBUG if args["debug"] else logging.INFO,
    filename=args["logfile"],
    format="%(asctime)s %(process)d %(levelname)s %(module)s '%(message)s'"
  )

  def SIGHANDLER(signum, frame):

    """
    Signal handler for SIGARLM during timeout
    """

    if signum == signal.SIGALRM:
      raise TimeoutError("SIGALRM received - process has timed out")

  def parallelWork(filename):
  
    """
    Work to be done in parallel by a process worker
    """

    # Set a signal handler for the child process to time out
    signal.signal(signal.SIGALRM, SIGHANDLER)
    signal.alarm(CONFIG["MULTIPROCESSING"]["TIMEOUT_SECONDS"])
  
    try:
      return WFMC.Process(filename)
    except Exception as ex:
      logging.critical("Exception (%s) for filename %s." % (ex, filename))
    finally:
      signal.alarm(0)
  
  def WFMCInitializer():

     global WFMC
     WFMC = WFMetadataCollector(args)


  def MultiProcess(files):

    """
    MultiProcess
    Starts pool of processes to handle WFMetadata ingestion
    """

    __counter = 1

    logging.info("Start multiprocessing of files.") 
    logging.info("Distributing work for %i files across %i workers." % (len(files), CONFIG["MULTIPROCESSING"]["NUMBER_OF_PROCESSES"]))

    # Create the processing pool
    processPool = Pool(CONFIG["MULTIPROCESSING"]["NUMBER_OF_PROCESSES"], initializer=WFMCInitializer)

    # Open the process pool and add neighbours to the set
    for _ in processPool.imap_unordered(parallelWork, files, chunksize=CONFIG["MULTIPROCESSING"]["CHUNK_SIZE"]):

      # Log progress on completed chunk
      __counter += 1
      if __counter % CONFIG["MULTIPROCESSING"]["CHUNK_SIZE"] == 0 :
        logging.info("Chunk completed. Metadata extraction progress at %.1f%%." % (100 * __counter / len(files)))

    # Close the process pool
    processPool.close()
    processPool.join()

  SDSFF = SDSFileFinder()

  # Get the input files from an SDS Archive
  inputFiles = SDSFF.FindFiles(args)

  WFDB = WFCatalogDB()
  inputFiles = WFDB.DependentFileChanged(inputFiles, args)

  # No work to be done
  if len(inputFiles) == 0:
    logging.info("No files found for processing"); sys.exit(0)

  # Multi or single process
  if CONFIG["MULTIPROCESSING"]["ENABLED"]:
    MultiProcess(inputFiles)
  else:
    logging.info("Multiprocessing is disabled")
    for file in inputFiles:
      parallelWork(file)
 
  logging.info("Master process has finished in %s" % (datetime.now() - initialized))

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

import patch_multiprocessing

def ParseArguments():

  """
  ParseArguments
  Parses command line arguments to dictionary
  """

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
  parser.add_argument("--version", help="View collector version", action="store_true")

  # Set custom logfile
  parser.add_argument("--logfile", help="Write output to logfile.", default=None)
  parser.add_argument("--debug", help="Set logging level to debug", action="store_true")

  # Force updates without checksum change
  parser.add_argument("--force", help="Force file updates.", action="store_true")

  # Delete documents from the database
  parser.add_argument("--delete", help="Remove files from the database", action="store_true")

  # Choice to store metrics and PSDs
  parser.add_argument("--store-psd", help="Add the spectral densities to the database.", action="store_true")
  parser.add_argument("--store-metrics", help="Add the metrics to the database.", action="store_true")

  return vars(parser.parse_args())


def signalHandler(signum, frame):

  """
  def signalHandler
  Signal handler for SIGARLM/SIGINT during timeout
  """

  if signum == signal.SIGALRM:
    raise TimeoutError("SIGALRM received - process has timed out")
  elif signum == signal.SIGINT:
    logging.critical("SIGINT received - shutting down worker")

def parallelWork(filename):

  """
  def parallelWork
  Work to be done in parallel by a process worker
  """

  signal.signal(signal.SIGALRM, signalHandler)
  signal.alarm(CONFIG["MULTIPROCESSING"]["TIMEOUT_SECONDS"])

  try:
    return WFMC.Process(filename)
  except Exception as e:
    logging.critical("Exception (%s) for filename %s." % (e, filename))
  finally:
    signal.alarm(0)


def WFMCInitializer():

  """
  WFMCInitializer
  Initializes a WFMetadataCollector once per process
  """

  global WFMC
  WFMC = WFMetadataCollector(args)

  # Set signals
  signal.signal(signal.SIGINT, signalHandler)


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

  try:

    # Open the process pool and add neighbours to the set
    for _ in processPool.imap_unordered(parallelWork, files, chunksize=CONFIG["MULTIPROCESSING"]["CHUNK_SIZE"]):

      # Log progress on completed chunk
      __counter += 1
      if __counter % CONFIG["MULTIPROCESSING"]["CHUNK_SIZE"] == 0 :
        logging.info("Chunk completed. Metadata extraction progress at %.1f%%." % (100 * __counter / len(files)))

    processPool.close()
  except KeyboardInterrupt:
    logging.critical("SIGINT received - pool will be terminated")
    processPool.terminate()
  finally:
    logging.info("Pool has been terminated.")
    processPool.join()

if __name__ == "__main__":

  initialized = datetime.now()

  # Get the command line arguments as a dict
  args = ParseArguments()

  # Print the application version
  if args["version"]:
    print(CONFIG["__VERSION__"]); sys.exit(0)
  if args["config"]:
    print(json.dumps(CONFIG, indent=4)); sys.exit(0)

  # Set up the logger
  logging.basicConfig(
    level=logging.DEBUG if args["debug"] else logging.INFO,
    filename=args["logfile"],
    format="%(asctime)s %(process)d %(levelname)s %(module)s '%(message)s'"
  )

  # Get the input files from an SDS Archive
  inputFiles = SDSFileFinder().FindFiles(args)
  inputFiles = WFCatalogDB().DependentFileChanged(inputFiles, args)

  # No work to be done
  if len(inputFiles) == 0:
    logging.info("No files found for processing"); sys.exit(0)

  # Multi or single process
  MultiProcess(inputFiles)
 
  logging.info("Master process has finished in %s" % (datetime.now() - initialized))

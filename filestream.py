"""

  Filestream classes for handling SDS type files and structures
  
  Author: Mathijs Koymans, 2017
  Copyright: ORFEUS Data Center, 2017

"""

import os
import json
import logging

from hashfn import md5, sha256
from obspy import UTCDateTime
from datetime import datetime, timedelta
from glob import glob
from fnmatch import fnmatch

from config import CONFIG

class SDSFile():

  """
  Public Class SDSFile
  Class for handling files in SDS archive

  Initialize by calling on a SDS Filename (e.g. GB.PGB1..HHN.D.2014.295)
  """

  def __init__(self, filename):

    """
    Attempt to create a filestream from a given filename
    """

    self._sha256 = None
    self._md5 = None

    self.filename = filename

    try:
      # Extract stream identification
      (self.network,
       self.station,
       self.location,
       self.channel,
       self.quality,
       self.year,
       self.day) = filename.split(".")

    except ValueError as ex:
      raise Exception("Input filename %s is not a valid SDS filename." % filename)

  # Returns filepath for a given file
  @property
  def filepath(self):
    return os.path.join(self.directory, self.filename)

  # Returns the stream identifier
  @property
  def id(self):
    return ".".join([
      self.network,
      self.station,
      self.location,
      self.channel
    ])

  # Returns the file directory based on SDS structure
  @property
  def directory(self):
    return os.path.join(
      CONFIG["ARCHIVE"]["ROOT"],
      self.year,
      self.network,
      self.station,
      self.channelDirectory
    )

  @property
  def datatype(self):
    if self.channel.endswith("DF"):
      return "infrasound"
    else:
      return "seismic"

  # Returns channel directory
  @property
  def channelDirectory(self):
    return self.channel + "." + self.quality

  # Returns next file in stream
  @property
  def next(self):
    return self._getAdjacentFile(1)

  # Returns previous file in stream
  @property
  def previous(self):
    return self._getAdjacentFile(-1)

  @property
  def size(self):
    return os.path.getsize(self.filepath)

  @property
  def modified(self):
    return os.path.getmtime(self.filepath)

  @property
  def md5(self):
    if self._md5 is None:
      with open(self.filepath, "rb") as f:
        self._md5 = md5(f)
    return self._md5

  @property
  def sha256(self):
    if self._sha256 is None:
      with open(self.filepath, "rb") as f:
        self._sha256 = sha256(f)
    return self._sha256

  @property
  def psdEnd(self):
    return self.end + timedelta(minutes=30)
 
  # Returns start time of file
  @property
  def start(self):
    return UTCDateTime(datetime.strptime(self.year + " " + self.day, "%Y %j"))

  # Returns end time of file
  @property
  def end(self):
    return UTCDateTime(self.start + timedelta(days=1))

  # Returns the file path of file
  @property
  def filepath(self):
    return os.path.join(CONFIG["ARCHIVE"]["ROOT"], self.directory, self.filename)

  # Returns list of files neighbouring a file
  @property
  def neighbours(self):
    return [SDSFile(os.path.basename(f)) for f in [self.previous.filepath, self.filepath, self.next.filepath] if os.path.isfile(f)]

  @property
  def filenames(self):
    return [f.filename for f in self.neighbours]

  @property
  def filepaths(self):
    return [f.filepath for f in self.neighbours]

  # Returns FDSNXML URL for resposnse request
  @property
  def FDSNXMLQuery(self):

    # If the location code is empty we are required to submit "--"
    # to the FDSN webservices
    return "".join([
      CONFIG["ARCHIVE"]["FDSN_STATION_PATH"],
      "?network=", self.network,
      "&station=", self.station,
      "&location=", "--" if self.location == "" else self.location,
      "&channel=", self.channel,
      "&start=", self.start.isoformat(),
      "&end=", self.end.isoformat(),
      "&level=response"
    ])


  def _getAdjacentFile(self, direction):

    """
    Private Function _getAdjacentFile
    Returns adjacent filestream based on direction
    """

    newDate = self.start + timedelta(days=direction)

    # The year and day may change
    newYear = newDate.strftime("%Y")
    newDay = newDate.strftime("%j")

    newFilename = ".".join([
      self.network,
      self.station,
      self.location,
      self.channel,
      self.quality,
      newYear,
      newDay
    ])

    return SDSFile(newFilename)


class SDSFileFinder():

  """
  Class SDSFileFinder
  Finds file in the SDS archive
  """

  def __init__(self):
    pass

  def FindFiles(self, args):

    """
    SDSFileFinder.GetFiles
    Returns filestreams based on input arguments
    """

    # Check if there is a single input method
    inputOptions = [
      args["date"],
      args["file"],
      args["dir"],
      args["list"],
      args["past"],
      args["glob"]
    ]

    # Raise exceptions and stop process
    if inputOptions.count(None) < len(inputOptions) - 1:
      raise Exception("Multiple file inputs options were passed; aborting.")

    # Different ways of inputting files
    if args["dir"] is not None:
      filestreams = self.GetFilesFromDirectory(args["dir"])
    elif args["list"] is not None:
      filestreams = self.GetFilesFromList(args["list"])
    elif args["file"] is not None:
      filestreams = self.GetFileFromFilename(args["file"])
    elif args["date"] is not None:
      filestreams = self.GetFilesFromDateRange(args["date"], args["range"])
    elif args["past"] is not None:
      filestreams = self.GetFilesFromPast(args["past"])
    elif args["glob"] is not None:
      filestreams = self.GetFilesFromGlobExpr(args["glob"])
    else:
      raise Exception("No input was given.");

    # If filtering is enabled
    if CONFIG["FILTERING"]["ENABLED"]:
      filestreams = filter(self.FilterFiles, filestreams)

    return filestreams


  def GetFilesFromDirectory(self, directory):

    """
    SDSFileFinder.GetFileFromDirectory
    Returns filestreams based on a (nested) directory
    """

    filestreams = list()

    # Not a valid directory
    if not os.path.isdir(directory):
      raise Exception("Input directory does not exist on the file system")

    # Recursively add all files
    for root, dirs, files in os.walk(directory):
      for file in files:
        filestream = SDSFile(file)
        if os.path.isfile(filestream.filepath):
          files.append(file)

    logging.info("Collected %i file(s) from directory %s" % (len(filestreams), directory))

    return filestreams


  def GetFilesFromPast(self, past):
  
    """
    SDSFileFinder.GetFilesFromPast
    Gets files from possible options from the past
    """
  
    if past == "today":
      window = -1
    elif past == "yesterday":
      window = -2
    elif past == "week":
      window = -8
    elif past == "fortnight":
      window = -15
    elif past == "month":
      window = -32
  
    filestreams = self.GetFilesFromDateRange(datetime.now().date(), window)
  
    logging.info("Collected %i file(s) from the past (%s)" % (len(filestreams), past))
  
    return filestreams


  def GetFilesFromDate(self, date):

    """
    SDSFileFinder.GetFilesFromDate
    Returns all files that belong to a given date
    """

    filestreams = list()

    # Get the year & day of year
    jday = date.strftime("%j")
    year = date.strftime("%Y")

    # Get the appropriate yearly directory
    directory = os.path.join(CONFIG["ARCHIVE"]["ROOT"], year)

    for root, dirs, files in os.walk(directory):
      for file in files:
        filestream = SDSFile(file)
        if filestream.day == jday and os.path.isfile(filestream.filepath):
          filestreams.append(file)

    logging.debug("Getting %i files from %s." % (len(filestreams), date))

    return filestreams


  def GetFilesFromDateRange(self, date, amount):
  
    """
    SDSFileFinder.GetFilesFromDateRange
    Returns all files that belong to a given date and range (amount)
    """
  
    filestreams = list()
  
    if isinstance(date, str):
      date = datetime.strptime(date, "%Y-%m-%d")
  
    # Include a given range (default to 1)
    for day in range(abs(amount)):
  
      if amount > 0:
        filestreams += self.GetFilesFromDate(date + timedelta(days=day))
      else:
        filestreams += self.GetFilesFromDate(date - timedelta(days=day))
  
    logging.info("Collected a total of %d file(s) from %d days." % (len(filestreams), abs(amount)))
  
    return filestreams


  def GetFileFromFilename(self, filename):

    """
    SDSFileFinder.GetFileFromFilename
    Returns filestream based on a single filename
    """

    filestream = SDSFile(filename)

    if not os.path.isfile(filestream.filepath):
      raise Exception("Input file %s does not exist on the file system." % filestream.filename)

    logging.info("Collected 1 file %s." % filestream.filename)

    return [filename]


  def GetFilesFromGlobExpr(self, wildcard):

    """
    SDSFileFinder.GetFilesFromGlobExpr
    Returns filestreams from a JSON formatted input list
    """

    # Attempt to create a phantom SDSFile using wildcards
    try:
      globExpression = SDSFile(wildcard).filepath
    except Exception:
      raise Exception("The glob expression is invalid and must be of the form: NETWORK.STATION.LOCATION.CHANNEL.QUALITY.YEAR.JDAY")

    logging.debug("Using glob expression: '%s'" % globExpression)

    # Expand the glob expression and get all files that match mapped to new SDSFiles
    filestreams = map(os.path.basename, glob(globExpression))

    logging.info("Collected %i files using a glob expression" % len(filestreams))

    return filestreams


  def GetFilesFromList(self, filelist):

    """
    SDSFileFinder.GetFilesFromList
    Returns filestreams from a JSON formatted input list
    """

    filestreams = json.loads(filelist)

    logging.info("Collected %i files from input list." % len(filestreams))

    return filestreams


  def FilterFiles(self, filename):

    """
    SDSFileFinder.FilterFiles
    Filters files based on a white list overruled by a black list    
    """

    for whiteFilter in CONFIG["FILTERING"]["WHITE_FILTER"]:
      if fnmatch(filename, whiteFilter):
        for blackFilter in CONFIG["FILTERING"]["BLACK_FILTER"]:
          if fnmatch(filename, blackFilter):
            logging.debug("Filtering file %s following rule %s." % (filename, blackFilter))
            return False
        return True
    return False

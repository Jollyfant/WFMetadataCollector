import os
import re
import logging

from pymongo import MongoClient
from filestream import SDSFile

from config import CONFIG

class WFCatalogDB():

  def __init__(self):

    """
    WFCatalogDB.__init__
    Initializes a new database connection
    """

    logging.info("Process with pid %i connecting to database at %s:%i." % (os.getpid(), CONFIG["MONGO"]["HOST"], CONFIG["MONGO"]["PORT"]))

    # Create a Mongo Client
    self.client = MongoClient(
      CONFIG["MONGO"]["HOST"],
      CONFIG["MONGO"]["PORT"]
    )

    self.database = self.client[CONFIG["MONGO"]["DATABASE"]]

    # Authenticate user
    if CONFIG["MONGO"]["AUTHENTICATE"]:
      self.Authenticate(
        CONFIG["MONGO"]["USER"],
        CONFIG["MONGO"]["PASS"]
      )


  def Authenticate(self, USER, PASS):

    """
    WFCatalogDB.Authenticate
    Authenticates user to the database
    """

    self.database.authenticate(USER, PASS)


  def GetMetadataByFilename(self, filename):

    """
    WFCatalogDB.SaveFileObject
    Saves a file object document to the file object collection
    """

    return self.database.metrics.find({"filename": filename})


  def SaveMetricObject(self, document):

    """
    WFCatalogDB.SaveFileObject
    Saves a file object document to the file object collection
    """

    return self.database.metrics.save(document)


  def RemoveSpectraObject(self, filename):

    return self.database.spectra.remove({"filename": filename})["n"]


  def RemoveMetricObject(self, filename):

    return self.database.metrics.remove({"filename": filename})["n"]


  def RemoveFileObject(self, filename):

    return self.database.fileObject.remove({"filename": filename})["n"]


  def GetFileObjects(self, filename):

    """
    WFCatalogDB.GetFileObjects
    Returns all file objects with a given filename
    """

    return self.database.fileObject.find({"filename": filename})

  def ConvertWildcards(self, expression):

    """
    Mongo.ConvertWildcards
    Changes glob expression supporting * and ? in to MongoDB wildcards
    """

    return "^" + expression.replace(".", "\.").replace("*", ".*").replace("?", ".?") + "$"

  def DeleteDocuments(self, wildcard):

    """
    Mongo.DeleteDocuments
    Removes documents matching a glob expression from the database
    """

    if not wildcard:
      raise Exception("A glob expression is required for document deletion")

    try:
      SDSFile(wildcard)
    except Exception as ex:
      raise Exception("The glob expression is invalid: %s" % ex)

    wildcard = self.ConvertWildcards(wildcard)
    regex = re.compile(wildcard, re.IGNORECASE)

    logging.info("Extracted %s from input glob expression" % wildcard)

    # Dry-running for now
    for document in self.database.fileObject.find({"filename": regex}):
      logging.info("Deleted document %s from collection fileObject" % document["filename"])

    for spectra in self.database.spectra.find({"filename": regex}):
      logging.info("Deleted document %s from collection spectra" % spectra["filename"])

    for metrics in self.database.spectra.find({"filename": regex}):
      logging.info("Deleted document %s from collection metrics" % metrics["filename"])
    

  def DependentFileChanged(self, filenames, args):

    """
    WFCatalogDB.DependentFileChanged
    Determines what files need to be updated due to changes in the file checksum
    This procedure simple reprocesses all files and neighbours if one thing changes

    VERY Important function!
    """

    # Open a unique set
    updateSet = set()

    # Go over all files, if the hash does not exist schedule update
    for filestream in map(SDSFile, filenames):

      # When the process is being forced update all neighbours
      if args["force"]:
        logging.debug("Forcing update on file %s and neighbours" % filestream.filename)
        updateSet.update(filestream.filenames)

      # Otherwise check the hash against the Digital Object in the database
      elif not self.HashExists(filestream.md5):
        logging.debug("Filename %s with hash %s does not exist as Digital Object. Adding %s to set for updating" % (filestream.filename, filestream.md5, filestream.filenames))
        updateSet.update(filestream.filenames)

      # No changes
      else:
        logging.debug("No md5 checksum changes detected for file %s" % filestream.filename)

    logging.info("The file set changed from %i input files to %i files for processing" % (len(filenames), len(updateSet)))

    return updateSet


  def HashExists(self, md5):

    """
    WFCatalogDB.HashExists
    Checks whether a given file md5 hash exists in the Digital Object database 
    """

    return self.GetFileObjectByHash(md5).count() != 0


  def SaveSpectraObject(self, document):

    """
    WFCatalogDB.SaveSpectraObject
    Saves all spectra objects to the spectra collection
    """

    self.database.spectra.save(document)


  def SaveFileObject(self, document):

    """
    WFCatalogDB.SaveFileObject
    Saves a file object document to the file object collection
    """

    return self.database.fileObject.save(document)


  def GetFileObjectByHash(self, fileHash):

    """
    WFCatalogDB.GetFileObjectByHash
    Returns the indexed file objects by md5 hash
    """

    return self.database.fileObject.find({"hash": fileHash})

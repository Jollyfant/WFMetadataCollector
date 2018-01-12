# System libs
import os
import logging
import warnings

# Libs
from struct import pack
from bson.binary import Binary
from datetime import datetime, timedelta
from obspy import read, read_inventory, Stream, UTCDateTime
from obspy.signal import PPSD
from obspy.signal.quality_control import MSEEDMetadata

# Custom modules
from filestream import SDSFile
from mongo import WFCatalogDB

# Configuration
from config import CONFIG

class WFMetadataCollector():

  """
  Main class
  """

  def __init__(self, args):

    self.initialized = datetime.now()
    logging.info("New WFMetadataCollector initialized with pid %i" % os.getpid())

    self.args = args
    self.database = WFCatalogDB()
    self.INVENTORY_CACHE = dict()

    self.PERIOD_TUPLE = (
      CONFIG["SPECTRA"]["MINIMUM_PERIOD"],
      CONFIG["SPECTRA"]["MAXIMUM_PERIOD"]
    )


  def Process(self, filename):

    """
    WFMetadataCollector.Process
    Process a given filestream to extract waveform metadata
    """

    self.processInitialized = datetime.now()

    filestream = SDSFile(filename)

    logging.info("Start processing file %s." % filestream.filename)
    logging.debug("Collected neighbouring files %s." % filestream.filenames)

    # Store the file object and get the version
    self.StoreDigitalObject(filestream)

    # Collect the waveform sample metrics
    if self.args["store_metrics"]:
      self.StoreMetricObject(filestream)

    # Collect power spectral densities
    if self.args["store_psd"]:
      self.StoreSpectraObject(filestream)


  def StoreSpectraObject(self, filestream):

    logging.debug("Start calculating spectra for %s" % filestream.filename)

    try:
      spectra = self.CollectPSD(filestream)
    except Exception as ex:
      return logging.critical("Critical exception (%s) during spectra calculation for filename %s" % (ex, filestream.filename))

    # Remove existing spectra from the database
    nRemoved = self.database.RemoveSpectraObject(filestream.filename)
    if nRemoved > 0:
      logging.info("Removed %i spectra object(s) from database for %s" % (nRemoved, filestream.filename))

    logging.info("Saving %i new spectra object(s) for %s" % (len(spectra), filestream.filename))

    for document in spectra:
      self.database.SaveSpectraObject(document)


  def StoreMetricObject(self, filestream):

    try:
      metrics = self.CollectMetrics(filestream)
    except Exception as ex:
      return logging.critical("Critical exception (%s) during metric calculation for filename %s." % (ex, filestream.filename))

    # Remove existing metric document from the database
    nRemoved = self.database.RemoveMetricObject(filestream.filename)
    if nRemoved > 0:
      logging.info("Removed %i metric object(s) from database for %s." % (nRemoved, filestream.filename))

    logging.info("Saving new metrics object for %s" % filestream.filename)

    self.database.SaveMetricObject(metrics)



  def CollectMetrics(self, filestream):

    """
    WFMetadataCollector.CollectMetrics
    Calls the ObsPy quality control function for WF metrics
    """

    with warnings.catch_warnings(record=True) as w:

      warnings.simplefilter("always")
      
      metadata = MSEEDMetadata(
        filestream.filepaths,
        starttime=filestream.start,
        endtime=filestream.end,
        add_flags=True,
        add_c_segments=True
      )

      metadata.meta.update({"warnings": len(w)})

    metadata.meta.update({"filename": filestream.filename})

    return self.GetDatabaseKeyMap(metadata.meta)


  def StoreDigitalObject(self, filestream):

    """
    WFMetadataCollector.StoreDigitalObject
    Stores a given file object to the file object database
    """

    # Create the file object literal
    fileObject = self.CreateFileObject(filestream)

    # Remove existing digital object
    if self.database.RemoveFileObject(filestream.filename) > 0:
      logging.info("Digital Object for %s removed from collection." % filestream.filename) 

    logging.info("Storing new Digital Object for %s." % filestream.filename) 

    return self.database.SaveFileObject(fileObject)


  def CreateFileObject(self, filestream):

    """
    WFMetadataCollector.CreateFileObject
    Creates a description of the file object to be stored in the object collection
    """

    # Add the file specific metadata
    return {
      "collector": CONFIG["__VERSION__"],
      "created": datetime.now(),
      "filename": filestream.filename,
      "size": filestream.size,
      "hash": filestream.sha256,
      "modified": filestream.modified,
      "date": filestream.start.datetime,
      "network": filestream.network,
      "station": filestream.station,
      "location": filestream.location,
      "channel": filestream.channel,
      "pid": None
    }


  def GetDatabaseKeyMap(self, trace):

    """
    WFMetadataCollector.GetDatabaseKeyMap
    Parses the returned metadata to MongoDB schema
    """
  
    # Source document for daily granules
    source = {
      "created": datetime.now(),
      "digitalObjects": trace["files"],
      "filename": trace["filename"],
      "collector": CONFIG["__VERSION__"],
      "warnings": trace["warnings"],
      "status": "open",
      "format": "miniSEED",
      "dataType": "seismic waveform",
      "nseg": len(trace.get("c_segments") or list()),
      "cont": trace["num_gaps"] == 0,
      "net": trace["network"],
      "sta": trace["station"],
      "cha": trace["channel"],
      "loc": trace["location"],
      "qlt": trace["quality"],
      "ts": trace["start_time"].datetime,
      "te": trace["end_time"].datetime,
      "enc": trace["encoding"],
      "srate": trace["sample_rate"],
      "rlen": trace["record_length"],
      "nrec": int(trace["num_records"]) if trace["num_records"] is not None else None,
      "nsam": int(trace["num_samples"]),
      "smin": int(trace["sample_min"]),
      "smax": int(trace["sample_max"]),
      "smean": float(trace["sample_mean"]),
      "smedian": float(trace["sample_median"]),
      "supper": float(trace["sample_upper_quartile"]),
      "slower": float(trace["sample_lower_quartile"]),
      "rms": float(trace["sample_rms"]),
      "stdev": float(trace["sample_stdev"]),
      "ngaps": int(trace["num_gaps"]),
      "glen": float(trace["sum_gaps"]),
      "nover": int(trace["num_overlaps"]),
      "olen": float(trace["sum_overlaps"]),
      "gmax": float(trace["max_gap"]) if trace["max_gap"] is not None else None,
      "omax": float(trace["max_overlap"]) if trace["max_overlap"] is not None else None,
      "avail": float(trace["percent_availability"]),
      "sgap": trace["start_gap"] is not None,
      "egap": trace["end_gap"] is not None
    }

    # Add the timing quality and flags
    source.update(self.GetTimingQuality(trace))
    source.update(self.GetFlags(trace))

    return source

  def GetFlags(self, trace):

    """
    WFMetadataCollector.GetFlags
    writes mSEED header flag percentages to source document
    """

    header = trace["miniseed_header_percentages"]

    source = {
      "io_flags": self.GetFlagKeys(header, "io_and_clock_flags"),
      "dq_flags": self.GetFlagKeys(header, "data_quality_flags"),
      "ac_flags": self.GetFlagKeys(header, "activity_flags")
    }

    return source


  def GetFlagKeys(self, trace, flag_type):

    """
    WFMetadataCollector.GetFlagKeys
    returns MongoDB document structure for miniseed header percentages
    """

    trace = trace[flag_type]

    if flag_type == "activity_flags":

      source = {
        "cas": float(trace["calibration_signal"]),
        "tca": float(trace["time_correction_applied"]),
        "evb": float(trace["event_begin"]),
        "eve": float(trace["event_end"]),
        "eip": float(trace["event_in_progress"]),
        "pol": float(trace["positive_leap"]),
        "nel": float(trace["negative_leap"])
      }

    elif flag_type == "data_quality_flags":

      source = {
        "asa": float(trace["amplifier_saturation"]),
        "dic": float(trace["digitizer_clipping"]),
        "spi": float(trace["spikes"]),
        "gli": float(trace["glitches"]),
        "mpd": float(trace["missing_padded_data"]),
        "tse": float(trace["telemetry_sync_error"]),
        "dfc": float(trace["digital_filter_charging"]),
        "stt": float(trace["suspect_time_tag"])
      }

    elif flag_type == "io_and_clock_flags":

      source = {
        "svo": float(trace["station_volume"]),
        "lrr": float(trace["long_record_read"]),
        "srr": float(trace["short_record_read"]),
        "sts": float(trace["start_time_series"]),
        "ets": float(trace["end_time_series"]),
        "clo": float(trace["clock_locked"])
      }

    return source


  def GetTimingQuality(self, trace):

    """
    WFMetadataCollector.GetTimingQuality
    Sets timing quality parameters to dictionary
    """

    trace = trace["miniseed_header_percentages"]

    # Add the timing correction
    source = {
      "tcorr": float(trace["timing_correction"])
    }

    # Check if the minimum is None, so is the rest
    # otherwise convert to floats
    if trace["timing_quality_min"] is None:

      source.update({
        "tqmin": None,
        "tqmax": None,
        "tqmean": None,
        "tqmedian": None,
        "tqupper": None,
        "tqlower": None
      })

    else:

      source.update({
        "tqmin": float(trace["timing_quality_min"]),
        "tqmax": float(trace["timing_quality_max"]),
        "tqmean": float(trace["timing_quality_mean"]),
        "tqmedian": float(trace["timing_quality_median"]),
        "tqupper": float(trace["timing_quality_upper_quartile"]),
        "tqlower": float(trace["timing_quality_lower_quartile"])
      })

    return source


  def ReadMSEED(self, filestream):

    """
    WFMetadataCollector.ReadMSEED
    Read miniSEED information from filestream padded on both sides
    """

    # Create an empty stream object
    obspyStream = Stream()

    # Collect data from the archive
    for neighbour in filestream.neighbours:

      st = read(neighbour.filepath,
                starttime=filestream.start,
                endtime=filestream.next.end,
                nearest_sample=False,
                format="MSEED")

      # Concatenate all traces
      for tr in st:
        if tr.stats.npts != 0:
          obspyStream.extend([tr])

    # No data was read
    if not obspyStream:
      raise Exception("No data within temporal constraints for %s." % filename)

    return obspyStream


  def CollectPSD(self, filestream):

    """
    WFMetadataCollector.CollectPSD
    Runs calculation to extract power spectral densities from a trace
    """

    # Skip infrasound channels
    if filestream.channel.endswith("DF"):
      raise Exception("Skipping infrasound channel %s." % filename)

    # Get the instrument response 
    inventory = self._GetInventory(filestream)

    # Create an empty ObsPy stream and fill it
    obspyStream = self.ReadMSEED(filestream)

    # Attempt to merge all streams with a fill value of 0
    obspyStream.merge(0, fill_value=0)

    # Single trace
    if len(obspyStream) > 1:
      raise Exception("More than a single stream detected in %s." % filename)

    # Trim the stream and pad values with 0
    # Include start and exclusive end
    obspyStream.trim(starttime=filestream.start,
                     endtime=filestream.psdEnd,
                     pad=True,
                     fill_value=0,
                     nearest_sample=False)

    trace = obspyStream[0]

    # Attempt to extract PSD values
    with warnings.catch_warnings(record=True) as w:

      warnings.simplefilter("always")

      # Call ObsPy PPSD class
      ppsd = PPSD(
        trace.stats,
        inventory,
        period_limits=self.PERIOD_TUPLE
      )

      ppsd.add(obspyStream)

      warn = len(w)

    logging.info("Calculated %i power spectral densities for %s." % (len(ppsd._binned_psds), filestream.filename))

    spectra = list()

    # Go over all returned segment and time steps
    for segment, time in zip(ppsd._binned_psds, self._Times(filestream.start)):

      # Concatenate offset with PSD values
      # Attempt to create a byte string
      try:
        byteAmplitudes = self._AsByteArray(self._GetOffset(segment, ppsd.valid))
      except Exception as ex:
        logging.error("Could not compress spectra for %s." % filestream.filename)
        continue

      spectra.append({
        "created": datetime.now(),
        "collector": CONFIG["__VERSION__"],
        "filename": filestream.filename,
        "net": filestream.network,
        "sta": filestream.station,
        "loc": filestream.location,
        "cha": filestream.channel,
        "warnings": warn,
        "ts": UTCDateTime(time).datetime,
        "te": (UTCDateTime(time) + timedelta(minutes=60)).datetime,
        "binary": byteAmplitudes
      })

    return spectra

  def _Times(self, start):

    """
    Returns 48 times starting at the start of the filestream
    with 30 minute increments
    """

    return [start + timedelta(minutes=(30 * x)) for x in range(48)]


  def _AsByteArray(self, array):

    """
    Private Function _AsByteArray
    Packs list of values 0 <= i <= 255 to byte array
    to be stored in MongoDB as an opaque binary string
    """

    return Binary("".join([pack("B", b) for b in array]))


  def _Reduce(self, x):

    """
    Private Function _Reduce
    Reduce the amplitude and truncate to integer

    Seismological data goes from 0 to -255
    Infrasound data goes from 100 to -155
    """

    x = int(x)

    if -255 <= x and x <= 0:
      return int(x + 255)
    else:
      return 255


  def _GetOffset(self, segment, mask):

    """
    Private Function _GetOffset
    Detects the first frequency and uses this offset
    """

    # Determine the first occurrence of True
    # from the Boolean mask, this will be the offset
    counter = 0
    for boolean in mask:
      if not boolean:
        counter += 1
      else:
        return [counter - 1] + [self._Reduce(x) for x in segment]


  def _GetInventory(self, filestream):

    """
    Public Property GetInventory
    Returns the stream inventory from cache or FDSN webservice
    """

    # Check if the inventory is cached
    if filestream.id not in self.INVENTORY_CACHE:

      # Attempt to get the inventory from the FDSN webservice
      # Set the cache
      self.INVENTORY_CACHE[filestream.id] = read_inventory(filestream.FDSNXMLQuery)

    return self.INVENTORY_CACHE[filestream.id]

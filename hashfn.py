import hashlib

BLOCK_SIZE = 1 << 16

def sha256(buffer):

  """
  SHA256
  Returns sha256 hexdigest
  """

  buffer.seek(0)
  sha256 = hashlib.sha256()

  for block in iter(lambda: buffer.read(BLOCK_SIZE), b""):
    sha256.update(block)

  return sha256.hexdigest()


def md5(buffer):

  """
  MD5
  Returns md5 hexdigest
  """

  buffer.seek(0)
  md5 = hashlib.md5()

  for block in iter(lambda: buffer.read(BLOCK_SIZE), b""):
    md5.update(block)

  return md5.hexdigest()

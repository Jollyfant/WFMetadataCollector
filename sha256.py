import hashlib

def sha256(buffer):

  BLOCK_SIZE = 1 << 16

  buffer.seek(0)
  sha256 = hashlib.sha256()

  for block in iter(lambda: buffer.read(BLOCK_SIZE), b""):
    sha256.update(block)

  return sha256.hexdigest()

  

"""
Wrapper function that fixes keyboards interrupts using multiprocessing.Pool

See:
- https://stackoverflow.com/questions/1408356/keyboard-interrupts-with-pythons-multiprocessing-pool
- https://gist.github.com/aljungberg/626518
    
"""

from multiprocessing.pool import IMapIterator

def wrapper(func):
  def wrap(self, timeout=None):
    return func(self, timeout=timeout if timeout is not None else 1e100)
  return wrap

IMapIterator.next = wrapper(IMapIterator.next)

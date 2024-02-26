import asyncio
import functools


def sync_async_bridge(func):
  """
  Decorator to make an async function compatible with both sync and async calls.
  """

  @functools.wraps(func)
  async def run_async(*args, **kwargs):
    return await func(*args, **kwargs)

  def run_sync(*args, **kwargs):
    loop = asyncio.get_event_loop()
    if loop.is_running():
      raise RuntimeError(
        "Trying to run async function in a running event loop. Use 'await' instead."
      )
    return loop.run_until_complete(func(*args, **kwargs))

  def wrapper(*args, **kwargs):
    if asyncio.iscoroutinefunction(func):
      try:  # Try to detect if we are in an async event loop
        loop = asyncio.get_running_loop()
        if loop.is_running():
          return run_async(*args, **kwargs)
      except RuntimeError:
        pass
      return run_sync(*args, **kwargs)
    else:
      return func(*args, **kwargs)

  return wrapper

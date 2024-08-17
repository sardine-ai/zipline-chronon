import asyncio
import inspect
from functools import wraps


def task(afunc=None, *exogenous_deps, raw=False):
    if raw and exogenous_deps:
        raise ValueError('Cannot have exogenous dependencies on a raw task.')

    def _decorate(wrapped_afunc):  # this layer is to allow `@task` or `@task(raw=True)`
        @wraps(wrapped_afunc)
        def wrapper(*args, **kwargs):
            async def _inner():
                callargs = inspect.signature(wrapped_afunc).bind(*args, **kwargs).arguments
                if raw:
                    for k, v in callargs.items():
                        if not inspect.isawaitable(v):
                            callargs[k] = asyncio.create_task(_task_wrapper(v))
                    if callargs:
                        await asyncio.wait(callargs.values())
                else:
                    gather_args = {}
                    non_gather_args = {}
                    for k, v in callargs.items():
                        if inspect.isawaitable(v):
                            gather_args[k] = v
                        else:
                            non_gather_args[k] = v

                    gather_args = dict(
                        zip(
                            gather_args.keys(),
                            await asyncio.gather(*gather_args.values())
                        )
                    )
                    callargs = {**gather_args, **non_gather_args}

                if exogenous_deps:
                    await asyncio.wait(exogenous_deps)

                return await wrapped_afunc(**callargs)
            return asyncio.create_task(_inner())
        return wrapper

    if afunc:
        return _decorate(afunc)

    return _decorate


async def _task_wrapper(val):
    return val

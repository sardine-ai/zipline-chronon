from graphlib import TopologicalSorter
import asyncio
import inspect
from functools import wraps

def _build_graph(nodes):
    graph = {}
    for node in nodes:
        node_name = node["name"]
        dependencies = node["dependencies"]
        graph[node_name] = set(dependencies)
    return graph

def _find_leaf_nodes(graph):
    all_nodes = set(graph.keys())
    dependent_nodes = set(node for dependencies in graph.values() for node in dependencies)
    leaf_nodes = all_nodes - dependent_nodes
    return leaf_nodes

def topological_sort(nodes):
    graph = _build_graph(nodes)
    ts = TopologicalSorter(graph)
    sorted_names = ts.static_order()
    leaf_nodes = _find_leaf_nodes(graph)
    #sorted_nodes = [[node for node in nodes if node['name'] == name][0] for name in sorted_names]
    sorted_nodes = []
    for name in sorted_names:
        original_node = [node for node in nodes if node['name'] == name][0]
        original_node["is_leaf"] = name in leaf_nodes
        sorted_nodes.append(original_node)
    return sorted_nodes


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

import asyncio
import uuid
from datetime import datetime, timedelta
import time
from ai.chronon.scheduler.interfaces.orchestrator import WorkflowOrchestrator
import subprocess
from threading import Event
from ai.chronon.scheduler.adapters.dag_helpers import topological_sort, task

from temporalio import workflow, activity
from temporalio import worker
from temporalio.client import Client
from temporalio.worker import Worker

@activity.defn
async def execute_command(command: str):
    # Runs async subprocess for `command`
    print(f"Starting command: {command}")
    """return await asyncio.create_subprocess_shell(
        command, 
        stdout=asyncio.subprocess.PIPE, 
        stderr=asyncio.subprocess.PIPE
    )"""
    return await asyncio.sleep(3)

@task
async def async_activity(command: str):
    return await workflow.execute_activity(execute_command, command, start_to_close_timeout=timedelta(days=1))
    

# Define a workflow class
@workflow.defn
class TemporalWorkflow:
    @workflow.run
    async def run(self, nodes):
        sorted_nodes = topological_sort(nodes)
        leaves = []
        name_to_tasks = {}
        for node in sorted_nodes:
            # Get dependencies -- relies on topologically sorted nodes
            dependency_tasks = [name_to_tasks[dep] for dep in node['dependencies']]
            node_task = task(async_activity, *dependency_tasks)(node['command'])
            name_to_tasks[node['name']] = node_task
            if node['is_leaf']:
                leaves.append(node_task)

        # only need to await the leaf nodes
        return await asyncio.gather(*leaves)


        """
        # prior implementation -- less optimal DAG exeuction
        completed_nodes = []
        results = []
        while len(completed_nodes) < len(nodes):
            next_batch = [node for node in nodes if (set(node["dependencies"]) <= set(completed_nodes) and node["name"] not in completed_nodes) ]
            actions = [workflow.execute_activity(execute_command, node['command'], start_to_close_timeout=timedelta(days=1)) for node in next_batch]
            batch_results = await asyncio.gather(*actions)
            results.append(batch_results)
            for node in next_batch:
                completed_nodes.append(node["name"])
        
        return results
        """

class TemporalAdapter(WorkflowOrchestrator):
    def __init__(self, dag_id, start_date, task_queue="temporal-orchestrator", client=None, target_host='host.docker.internal:7233'):
        self.name = dag_id + "_" + start_date + str(uuid.uuid4())
        self.task_queue = task_queue
        self.target_host = target_host

    def setup(self):
        # Handled in constructor
        pass

    def schedule_task(self, node):
        # Handled in workflow run
        pass
        
    def set_dependencies(self, task, dependencies):
        # Handled in workflow run
        pass

    def build_dag_from_flow(self, flow):
        # Flow used directly in workflow run, needs to be JSON serializable for Temporal
        self.nodes = [node.to_dict() for node in flow.nodes]
        print([node["name"] for node in self.nodes])

    async def trigger_run_async(self):
        """Trigger the workflow run"""
        # Start the workflow
        client = await Client.connect(self.target_host, namespace="default")

        async with Worker(
            client,
            task_queue="temporal-orchestrator",
            workflows=[TemporalWorkflow],
            activities=[execute_command]
        ):

            result = await client.execute_workflow(
                TemporalWorkflow.run,
                args=(self.nodes,),
                task_queue=self.task_queue,
                id=self.name
            )
            return result
    
    def trigger_run(self):
        print([node["name"] for node in self.nodes])
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(self.trigger_run_async())
        except KeyboardInterrupt:
            return

        #return asyncio.run(self.trigger_run_async())

-------------------------- MODULE ChrononOrchestrator --------------------------
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Nodes,           \* Set of node identifiers
    PartitionRange,  \* Set of partition ranges (simplified as integers)
    MaxRequests,     \* Maximum number of concurrent requests
    MaxSteps         \* Maximum step size for bounded model checking

VARIABLES
    \* Core state variables
    executions,      \* Set of execution records
    dependencies,    \* Dependency relationships between executions  
    nodeConfigs,     \* Node configuration including step sizes and dependencies
    requesters,      \* Reference counting for cancellation
    
    \* Control variables for model checking
    nextExecutionId, \* Counter for unique execution IDs
    activeRequests   \* Set of active workflow requests

vars == <<executions, dependencies, nodeConfigs, requesters, nextExecutionId, activeRequests>>

\* Execution statuses
ExecutionStatus == {"WAITING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED"}

\* Execution record structure
ExecutionRecord == [
    id: Nat,
    nodeKey: Nodes,
    startPartition: PartitionRange,
    endPartition: PartitionRange,
    status: ExecutionStatus,
    requesterCount: Nat
]

\* Node configuration structure  
NodeConfig == [
    stepSize: 1..MaxSteps,
    tableDependencies: SUBSET Nodes
]

\* Request structure for workflow initiation
WorkflowRequest == [
    id: Nat,
    targetNode: Nodes,
    startPartition: PartitionRange,
    endPartition: PartitionRange
]

------------------------------------------------------------------------------
\* Initial state
Init ==
    /\ executions = {}
    /\ dependencies = {}
    /\ nodeConfigs \in [Nodes -> NodeConfig]
    /\ requesters = {}
    /\ nextExecutionId = 1
    /\ activeRequests = {}

------------------------------------------------------------------------------
\* Helper predicates and operators

\* Check if two partition ranges overlap
PartitionOverlap(r1, r2) ==
    /\ r1.start <= r2.end
    /\ r2.start <= r1.end

\* Compute union of two overlapping ranges
PartitionUnion(r1, r2) ==
    [start |-> IF r1.start < r2.start THEN r1.start ELSE r2.start, 
     end |-> IF r1.end > r2.end THEN r1.end ELSE r2.end]

\* Find executions for a node that overlap with given range
OverlappingExecutions(nodeKey, startP, endP) ==
    {e \in executions : 
        /\ e.nodeKey = nodeKey
        /\ e.status \in {"WAITING", "RUNNING"}
        /\ PartitionOverlap([start |-> e.startPartition, end |-> e.endPartition],
                           [start |-> startP, end |-> endP])}

\* Check if execution is active (not terminal)
IsActive(execution) ==
    execution.status \in {"WAITING", "RUNNING"}

\* Get upstream dependencies for a node
UpstreamNodes(nodeKey) ==
    nodeConfigs[nodeKey].tableDependencies

------------------------------------------------------------------------------
\* Action: Schedule a new workflow request
ScheduleWorkflow ==
    /\ Cardinality(activeRequests) < MaxRequests
    /\ \E targetNode \in Nodes, startP \in PartitionRange, endP \in PartitionRange :
        /\ startP <= endP
        /\ LET request == [id |-> nextExecutionId, 
                          targetNode |-> targetNode,
                          startPartition |-> startP,
                          endPartition |-> endP]
           IN /\ activeRequests' = activeRequests \cup {request}
              /\ nextExecutionId' = nextExecutionId + 1
              /\ UNCHANGED <<executions, dependencies, nodeConfigs, requesters>>

------------------------------------------------------------------------------
\* Action: Process a workflow request by scheduling execution
ProcessWorkflowRequest ==
    /\ activeRequests # {}
    /\ \E request \in activeRequests :
        LET nodeKey == request.targetNode
            startP == request.startPartition  
            endP == request.endPartition
            overlapping == OverlappingExecutions(nodeKey, startP, endP)
        IN
        \/ \* Case 1: Existing active execution with exact range - join it
           /\ \E existing \in overlapping :
                /\ existing.startPartition = startP
                /\ existing.endPartition = endP
                /\ IsActive(existing)
           /\ requesters' = requesters \cup {[executionId |-> CHOOSE e \in overlapping : 
                                              e.startPartition = startP /\ e.endPartition = endP,
                                            requesterId |-> request.id]}
           /\ activeRequests' = activeRequests \ {request}
           /\ UNCHANGED <<executions, dependencies, nodeConfigs, nextExecutionId>>
           
        \/ \* Case 2: Overlapping executions exist - compute union and reschedule
           /\ overlapping # {}
           /\ ~\E existing \in overlapping : 
                existing.startPartition = startP /\ existing.endPartition = endP
           /\ LET unionRange == PartitionUnion([start |-> startP, end |-> endP],
                                             CHOOSE r \in {[start |-> e.startPartition, end |-> e.endPartition] : e \in overlapping} : TRUE)
                  newExecution == [id |-> nextExecutionId,
                                  nodeKey |-> nodeKey,
                                  startPartition |-> unionRange.start,
                                  endPartition |-> unionRange.end,
                                  status |-> "WAITING",
                                  requesterCount |-> 1]
              IN /\ executions' = (executions \ overlapping) \cup {newExecution}
                 /\ requesters' = requesters \cup {[executionId |-> nextExecutionId, requesterId |-> request.id]}
                 /\ nextExecutionId' = nextExecutionId + 1
                 /\ activeRequests' = activeRequests \ {request}
                 /\ UNCHANGED <<dependencies, nodeConfigs>>
                 
        \/ \* Case 3: No overlapping executions - create new execution
           /\ overlapping = {}
           /\ LET newExecution == [id |-> nextExecutionId,
                                  nodeKey |-> nodeKey,
                                  startPartition |-> startP,
                                  endPartition |-> endP,
                                  status |-> "WAITING",
                                  requesterCount |-> 1]
              IN /\ executions' = executions \cup {newExecution}
                 /\ requesters' = requesters \cup {[executionId |-> nextExecutionId, requesterId |-> request.id]}
                 /\ nextExecutionId' = nextExecutionId + 1
                 /\ activeRequests' = activeRequests \ {request}
                 /\ UNCHANGED <<dependencies, nodeConfigs>>

------------------------------------------------------------------------------
\* Action: Start execution after dependencies are met
StartExecution ==
    /\ \E execution \in executions :
        /\ execution.status = "WAITING"
        /\ LET upstreamNodes == UpstreamNodes(execution.nodeKey)
               upstreamExecutions == {e \in executions : e.nodeKey \in upstreamNodes}
               requiredUpstream == {e \in upstreamExecutions : 
                                   \* Simplified dependency check - upstream must cover our range
                                   e.startPartition <= execution.startPartition /\
                                   e.endPartition >= execution.endPartition}
           IN /\ \A e \in requiredUpstream : e.status = "SUCCEEDED"
              /\ executions' = (executions \ {execution}) \cup 
                              {[execution EXCEPT !.status = "RUNNING"]}
              /\ dependencies' = dependencies \cup 
                               {[downstream |-> execution.id, upstream |-> e.id] : e \in requiredUpstream}
              /\ UNCHANGED <<nodeConfigs, requesters, nextExecutionId, activeRequests>>

------------------------------------------------------------------------------
\* Action: Complete execution successfully
CompleteExecution ==
    /\ \E execution \in executions :
        /\ execution.status = "RUNNING"
        /\ executions' = (executions \ {execution}) \cup 
                        {[execution EXCEPT !.status = "SUCCEEDED"]}
        /\ UNCHANGED <<dependencies, nodeConfigs, requesters, nextExecutionId, activeRequests>>

------------------------------------------------------------------------------
\* Action: Fail execution
FailExecution ==
    /\ \E execution \in executions :
        /\ execution.status = "RUNNING"
        /\ executions' = (executions \ {execution}) \cup 
                        {[execution EXCEPT !.status = "FAILED"]}
        /\ UNCHANGED <<dependencies, nodeConfigs, requesters, nextExecutionId, activeRequests>>

------------------------------------------------------------------------------
\* Action: Cancel execution (reference counted)
CancelExecution ==
    /\ \E execution \in executions, requester \in requesters :
        /\ IsActive(execution)
        /\ requester.executionId = execution.id
        /\ \/ \* Last requester - cancel execution
              /\ execution.requesterCount = 1
              /\ executions' = (executions \ {execution}) \cup 
                              {[execution EXCEPT !.status = "CANCELLED", !.requesterCount = 0]}
              /\ requesters' = requesters \ {requester}
              /\ UNCHANGED <<dependencies, nodeConfigs, nextExecutionId, activeRequests>>
           \/ \* Multiple requesters - decrement count
              /\ execution.requesterCount > 1
              /\ executions' = (executions \ {execution}) \cup 
                              {[execution EXCEPT !.requesterCount = @ - 1]}
              /\ requesters' = requesters \ {requester}
              /\ UNCHANGED <<dependencies, nodeConfigs, nextExecutionId, activeRequests>>

------------------------------------------------------------------------------
\* Next state action
Next ==
    \/ ScheduleWorkflow
    \/ ProcessWorkflowRequest
    \/ StartExecution
    \/ CompleteExecution
    \/ FailExecution
    \/ CancelExecution

------------------------------------------------------------------------------
\* State space constraint for model checking
StateConstraint ==
    /\ Cardinality(executions) <= 4
    /\ Cardinality(activeRequests) <= MaxRequests  
    /\ nextExecutionId <= 6

------------------------------------------------------------------------------
\* Specification
Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

------------------------------------------------------------------------------
\* Invariants to verify correctness

\* Safety: No execution can be in multiple statuses simultaneously
ExecutionStatusConsistency ==
    \A e \in executions : e.status \in ExecutionStatus

\* Safety: Reference counting is consistent
ReferenceCountConsistency ==
    \A e \in executions :
        e.requesterCount = Cardinality({r \in requesters : r.executionId = e.id})

\* Safety: No race conditions - overlapping ranges for same node must be unified
NoRaceConditions ==
    \A e1, e2 \in executions :
        /\ e1 # e2
        /\ e1.nodeKey = e2.nodeKey
        /\ IsActive(e1) /\ IsActive(e2)
        => ~PartitionOverlap([start |-> e1.startPartition, end |-> e1.endPartition],
                           [start |-> e2.startPartition, end |-> e2.endPartition])

\* Safety: Dependencies respect execution ordering
DependencyOrdering ==
    \A dep \in dependencies :
        \E upstream, downstream \in executions :
            /\ upstream.id = dep.upstream
            /\ downstream.id = dep.downstream
            /\ upstream.status \in {"SUCCEEDED", "FAILED", "CANCELLED"}
               => downstream.status # "RUNNING"

\* Liveness: All waiting executions eventually start or get cancelled
EventualProgress ==
    [](\A e \in executions : 
        e.status = "WAITING" => <>(e.status \in {"RUNNING", "CANCELLED"}))

\* Liveness: All running executions eventually complete  
EventualCompletion ==
    [](\A e \in executions :
        e.status = "RUNNING" => <>(e.status \in {"SUCCEEDED", "FAILED", "CANCELLED"}))

==============================================================================
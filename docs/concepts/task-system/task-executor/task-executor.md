# Task Executor

Task executor is the component which is responsible for executing the task graph. A process model or a hybrid model with threads can be used for execution which is based on the system specification. 
It is important to handle both I/O and task execution within a single execution module so that the framework can achieve the best possible performance by overlapping I/O and computations. 
The execution is responsible for managing the scheduled tasks and activating them with data coming from the message layer. Unlike in MPI based applications where threads are created equal to the number of CPU cores, big data systems typically employ more threads than the cores available to facilitate I/O operations. 

* Task Executor is the component which is responsible for executing the tasks that are submitted through the task scheduler in each worker
  * It uses threads to execute a given task plan.
  * It allows to run one or more executors run on each worker node
  * It will queue the tasks and execute the tasks based on the submitted order. 
* The task executor will receive the tasks as serialized objects and it will deserialize the objects before processing them. 
* A thread pool will be maintained by the task executors to manage the core in an optimal manner. 
  * The size of the thread pool will be determined by the number of cores that are available to the executor. 

# Types of Task Executors

* Task Executor is implemented with two types of executors namely
  * Batch Sharing Task Executor
  * Streaming Sharing Task Executor
* Task Executor invokes the appropriate task executors based on the type of the task graph.
* Batch Sharing Task Executor terminate after the computation ends whereas Streaming Sharing Task Executor runs continuously. 

# Task Executor Call

```text
public Executor(Config cfg, int wId, ExecutionPlan executionPlan, TWSChannel channel, OperationMode operationMode)
```

# Execution Graph

* Execution graph is a transformation of the user-defined task graph created by the framework to be executed on the cluster.
* Execution graph will be scheduled to the available resource by the task scheduler.
* Fig.1 represents the user graph and execution graph where they run multiple ‘W’ and ‘G’ operations.

# Executor

Executor is the component responsible for executing the task graph. It uses threads to execute a given
task plan.

```java
public Executor(Config cfg, int wId, ExecutionPlan executionPlan,
                  TWSChannel channel, OperationMode operationMode)
```

Depending on the task graph, it can use a streaming executor or a batch executor.

Batch executors terminate after the computation ends and streaming executors do not terminate.





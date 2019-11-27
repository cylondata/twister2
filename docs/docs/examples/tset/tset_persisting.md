---
id: tset_persisting
title: TSet Persisting
sidebar_label: TSet Persisting / Checkpointing
---

## About this example

Everything we discussed so far happens on memory. When our data doesn't fit into the memory, persist() command can be used to offload data directly into the local disk. This makes all previous residuals garbage collectible and helps to free up memory for the upcoming operations. 

## TSet Persist Operation

Other than giving the ability to use disk, persist() command serves another purpose when it comes to fault tolerence. If you run twister2 with fault tolerence enabled, when persist() command is executed, twister2 internally creates a checkpoint and on failure, you can restart the same job from the last persist()ed point.

<!--DOCUSAURUS_CODE_TABS-->
<!--Java-->
```java
SourceTSet<Integer> sourceX = env.createSource(new SourceFunc<Integer>() {

      private int count = 0;

      @Override
      public boolean hasNext() {
        return count < 1000000;
      }

      @Override
      public Integer next() {
        return count++;
      }
    }, 4);

    long t1 = System.currentTimeMillis();
    ComputeTSet<Object, Iterator<Object>> twoComputes = sourceX.direct().compute((itr, c) -> {
      itr.forEachRemaining(i -> {
        c.collect(i * 5);
      });
    }).direct().compute((itr, c) -> {
      itr.forEachRemaining(i -> {
        c.collect((int) i + 2);
      });
    });
    LOG.info("Time for two computes : " + (System.currentTimeMillis() - t1));

    t1 = System.currentTimeMillis();
    PersistedTSet<Object> persist = twoComputes.persist();
    LOG.info("TIme for cache : " + (System.currentTimeMillis() - t1));
    // When persist() is called, twister2 performs all the computations/communication
    // upto this point and persists the result into the disk.
    // This makes previous data garbage collectible and frees some memory.
    // If persist() is called in a checkpointing enabled job, this will create
    // a snapshot at this point and will start straightaway from this point if the
    // job is restarted.

    // Similar to CachedTSets, PersistedTSets can be added as inputs for other TSets and
    // operations


    persist.reduce((i1, i2) -> {
      return (int) i1 + (int) i2;
    }).forEach(i -> {
      LOG.info("SUM=" + i);
    });
```

<!--Python-->
```python
class IntSource(SourceFunc):

    def __init__(self):
        super().__init__()
        self.i = 0

    def has_next(self):
        return self.i < 10000

    def next(self):
        res = self.i
        self.i = self.i + 1
        return res


source_x = env.create_source(IntSource(), 4)


def mul_by_five(itr, collector, ctx: TSetContext):
    for i in itr:
        collector.collect(i * 5)


def add_two(itr, collector, ctx: TSetContext):
    for i in itr:
        collector.collect(i + 2)


t1 = datetime.now()
two_computes = source_x.compute(mul_by_five).compute(add_two)
t2 = datetime.now()
print("Time taken for two_computes %d" % (t2 - t1).total_seconds())

t1 = datetime.now()
persisted = two_computes.persist()
t2 = datetime.now()
print("Time taken for cache %d" % (t2 - t1).total_seconds())

persisted.reduce(lambda i1, i2: i1 + i2) \
    .for_each(lambda i: print("SUM = %d" % i))
```
<!--END_DOCUSAURUS_CODE_TABS-->

## Running this example

<!--DOCUSAURUS_CODE_TABS-->
<!--Java-->
```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.tset.tutorial.intermediate.checkpoint.TSetCheckptExample
```

<!--Python-->
```bash
./bin/twister2 submit standalone python examples/python/tset_checkpointing.py
```
<!--END_DOCUSAURUS_CODE_TABS-->

## Output

We should see 4 responses with each worker with the time taken for persisting and a response from 0th worker with the sum of all the numbers.

<!--DOCUSAURUS_CODE_TABS-->
<!--Java-->
Here sum could be negative due to int overflow.
```bash
[2019-11-27 14:22:54 -0500] [INFO] [worker-3] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.checkpoint.TSetCheckptExample: Time for persist : 1  
[2019-11-27 14:22:54 -0500] [INFO] [worker-2] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.checkpoint.TSetCheckptExample: Time for persist : 1 
[2019-11-27 14:22:54 -0500] [INFO] [worker-1] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.checkpoint.TSetCheckptExample: Time for persist : 1  
[2019-11-27 14:22:54 -0500] [INFO] [worker-0] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.checkpoint.TSetCheckptExample: Time for persist : 1 
[2019-11-27 14:23:36 -0500] [INFO] [worker-0] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.checkpoint.TSetCheckptExample: SUM=-1378487240
```

<!--Python-->
```bash
[2019-11-27 13:55:10 -0500] [INFO] [worker-3] [python-process] edu.iu.dsc.tws.python.PythonWorker: Time taken for persist 5  
[2019-11-27 13:55:10 -0500] [INFO] [worker-0] [python-process] edu.iu.dsc.tws.python.PythonWorker: Time taken for persist 4 
[2019-11-27 13:55:09 -0500] [INFO] [worker-2] [python-process] edu.iu.dsc.tws.python.PythonWorker: Time taken for persist 5  
[2019-11-27 13:55:10 -0500] [INFO] [worker-1] [python-process] edu.iu.dsc.tws.python.PythonWorker: Time taken for persist 4  
[2019-11-27 13:55:10 -0500] [INFO] [-] [JM] edu.iu.dsc.tws.master.server.WorkerMonitor: Worker:3 COMPLETED.  
SUM = 999980000
```
<!--END_DOCUSAURUS_CODE_TABS-->

## Checkpointing

Other than for offloading data to the disk, persis() can be used for checkpointing. When checkpointed, jobs can be restarted from the checkpoints on failures, without having to execute everything from scratch.

To enable checkpointing `twister2.checkpointing.enable` configuration of `checkpoint.yaml` should be switched to `true`. Other than that, following language specific changes should be applied to your code.

<!--DOCUSAURUS_CODE_TABS-->
<!--Java-->
Extend your Worker from `CheckpointingBatchTSetIWorker` instead of `BatchTSetIWorker`.
```bash
public class TSetCheckptExample implements CheckpointingBatchTSetIWorker, Serializable {

}
```

<!--Python-->
There is nothing to be changed. Checkpointing will get automatically enabled when `checkpoint.yaml` is changed


<!--END_DOCUSAURUS_CODE_TABS-->

## Restarting a checkpointed job

Every twister2 job has an ID. The ID of the twister2 job can be found in initial set of logs.

```bash
 _____           _     _           ____  
/__   \__      _(_)___| |_ ___ _ _|___ \ 
  / /\/\ \ /\ / / / __| __/ _ \ '__|__) |
 / /    \ V  V /| \__ \ ||  __/ |  / __/ 
 \/      \_/\_/ |_|___/\__\___|_| |_____| v0.4.0
                                         
Job Name        :       python-job-9d98a98e-779c-497a-91b9-486b50b01bc6
Job ID          :       python-job-9d98a98e-779c-497a-91b9-486b50b01bc6-a1594862-8b5e-4437-b7f3-e8cccfa9e6fc
Cluster Type    :       standalone
Runtime         :       OpenJDK 64-Bit Server VM 12.0.2+10
```

In above case, the job id is `python-job-9d98a98e-779c-497a-91b9-486b50b01bc6-a1594862-8b5e-4437-b7f3-e8cccfa9e6fc`

A checkpointed job can be restarted by proving the job id to the `restart` command as follows.

```bash
./bin/twister2 restart standalone python-job-9d98a98e-779c-497a-91b9-486b50b01bc6-a1594862-8b5e-4437-b7f3-e8cccfa9e6fc
```

## Output of restarted job

Restarted jobs take close to 0sec for persist() operation as they are just creating a pointer to the previous results from the disk. 

<!--DOCUSAURUS_CODE_TABS-->
<!--Java-->
```bash
[2019-11-27 14:25:41 -0500] [INFO] [worker-0] [main] edu.iu.dsc.tws.executor.core.ExecutionPlanBuilder: Tasks will start with version 0  
[2019-11-27 14:25:41 -0500] [INFO] [worker-2] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.checkpoint.TSetCheckptExample: Time for persist : 0  
[2019-11-27 14:25:41 -0500] [INFO] [worker-3] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.checkpoint.TSetCheckptExample: Time for persist : 0  
[2019-11-27 14:25:41 -0500] [INFO] [worker-1] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.checkpoint.TSetCheckptExample: Time for persist : 0  
[2019-11-27 14:25:41 -0500] [INFO] [worker-0] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.checkpoint.TSetCheckptExample: Time for persist : 0 
...
[2019-11-27 14:26:26 -0500] [INFO] [worker-0] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.checkpoint.TSetCheckptExample: SUM=-1378487240 
```

<!--Python-->
```bash
[2019-11-27 13:55:10 -0500] [INFO] [worker-3] [python-process] edu.iu.dsc.tws.python.PythonWorker: Time taken for persist 0  
[2019-11-27 13:55:10 -0500] [INFO] [worker-0] [python-process] edu.iu.dsc.tws.python.PythonWorker: Time taken for persist 0 
[2019-11-27 13:55:09 -0500] [INFO] [worker-2] [python-process] edu.iu.dsc.tws.python.PythonWorker: Time taken for persist 0  
[2019-11-27 13:55:10 -0500] [INFO] [worker-1] [python-process] edu.iu.dsc.tws.python.PythonWorker: Time taken for persist 0  
[2019-11-27 13:55:10 -0500] [INFO] [-] [JM] edu.iu.dsc.tws.master.server.WorkerMonitor: Worker:3 COMPLETED.  
SUM = 999980000
```
<!--END_DOCUSAURUS_CODE_TABS-->




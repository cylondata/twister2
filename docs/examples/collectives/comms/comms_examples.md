# Communication Examples

Communication examples demonstrate the usage of communication API. 
The communication api has the capability of creating the communication among the
tasks by abstracting the communication logic to the api users. 
Basically, the communication layer does the job of creating the communication of the tasks in the 
task graph based on the user description. Communication api supports a simple thread model
to spawn the processes to run the the tasks using built communication. The
communication logic is visible to the users when the communication API is used.
Twister2 has the modular structure for supporting the developer's need. They can 
plug in the Twister2:Net to support the communication in the projects without using
the other layers in Twister2. In the communication examples we demonstrate how to 
create the following collective communications. 

## Twister2 Collective Communications

1. Reduce
2. Allreduce
3. Gather
4. AllGather
5. KeyedReduce
6. KeyedGather
7. Broadcast
8. Partition
9. KeyedPartition

After building the project, you can run the batch mode examples as follows. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.comms.ExampleMain -itr <iterations> -workers <workers> -size <data_size> -op "<operation>" -stages <source_parallelsim>,<sink_parallelism> -<flag> -verify
```

### Running Option Definitions

1. itr : integer number which provides the number of iterations which the application must run
2. workers : Number of workers needed for the application
3. data_size : size of the array that is being passed. In the examples, we generate an array specified by this size
4. op : Collective operation type:
    1. reduce
    2. allreduce
    3. gather
    4. allgather
    5. keyed-reduce
    6. keyed-gather
    7. bcast
    8. partition
    9. keyed-partition
5. stages : has to params 
    1. source_parallelism : depending on the collective communication that you prefer select this number
    2. sink_parallelism : depending on the collective communication that you prefer select this number
    Example : For reduce communication it can be 4,1 as sink has to be a single point of receive in the reduction 
    process and source has more data generating points. 
6. flag : for a batch job this flag must be empty, if it is an streaming job, -stream would state the flag. 
7. verify : -verify flag does the verification of the collective operation. This is an optional parameter. 


### Communication Example Structure

The main component of the example is in the execute method of each example class. 
Initially, a task plan is created 

```bash
 protected void execute() {
    TaskPlan taskPlan = Utils.createStageTaskPlan(config, resourcePlan,
        jobParameters.getTaskStages(), workerList);

    Set<Integer> sources = new HashSet<>();
    Integer noOfSourceTasks = jobParameters.getTaskStages().get(0);
    for (int i = 0; i < noOfSourceTasks; i++) {
      sources.add(i);
    }
    int target = noOfSourceTasks;
    // create the communication
    reduce = new BReduce(communicator, taskPlan, sources, target,
        new ReduceOperationFunction(Op.SUM, MessageType.INTEGER), new FinalSingularReceiver(),
        MessageType.INTEGER);

    Set<Integer> tasksOfExecutor = Utils.getTasksOfExecutor(workerId, taskPlan,
        jobParameters.getTaskStages(), 0);
    for (int t : tasksOfExecutor) {
      finishedSources.put(t, false);
    }
    if (tasksOfExecutor.size() == 0) {
      sourcesDone = true;
    }

    if (!taskPlan.getChannelsOfExecutor(workerId).contains(target)) {
      reduceDone = true;
    }

    LOG.log(Level.INFO, String.format("%d Sources %s target %d this %s",
        workerId, sources, target, tasksOfExecutor));
    // now initialize the workers
    for (int t : tasksOfExecutor) {
      // the map thread where data is produced
      Thread mapThread = new Thread(new MapWorker(t));
      mapThread.start();
    }
  }

```

First step is to create the task plan and the workers associated with the example 
configuration is provided. 

```bash
 TaskPlan taskPlan = Utils.createStageTaskPlan(config, resourcePlan,
        jobParameters.getTaskStages(), workerList);
```

Then we need to provide the collective communication type that is going to be used.
Along with the final data receiver setting and the data types used in the messaging. 

```bash
 reduce = new BReduce(communicator, taskPlan, sources, target,
        new ReduceOperationFunction(Op.SUM, MessageType.INTEGER), new FinalSingularReceiver(),
        MessageType.INTEGER);
``` 

The rest of the logic included in the example, is designed to identify the tasks which have
finished the communication and execution of the tasks. This logic is being encapsulated in 
the task layer examples which are in a very simple format. Through out the collective communication
examples, the main changing component is the collective communication defining section. 

## Batch Examples

### Reduce Example

In the reduction operation, we need specify which kind of mathematical operation that
we are expecting to run, in this case the summation is considered for a integer data set
generated by the program itself. In the following code snippet, the reduction based 
communication is built based on the user inputs when running the application. 

```java
    reduce = new BReduce(communicator, taskPlan, sources, target,
        new ReduceOperationFunction(Op.SUM, MessageType.INTEGER), new FinalSingularReceiver(),
        MessageType.INTEGER);
```

Running a reduction operation on an array of size 8 array with 4 workers iterating once with source parallelism of 8
and sink parallelism of 1, added with result verification. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.comms.ExampleMain -itr 1 -workers 4 -size 8 -op "reduce" -stages 8,1 -verify
```

[Communication based Batch Reduce Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/comms/batch/BReduceExample.java)


### AllReduce Example

In the allreduce operation the example is very similar with the reduce example, but the only change
is the number of stages. The stages in allreduce must obey the condition that source parallelism
and sink parallelism must be equal. 

The communication defining section is as follows,

```bash
 reduce = new BAllReduce(communicator, taskPlan, sources, targets,
        new ReduceOperationFunction(Op.SUM, MessageType.INTEGER), new FinalSingularReceiver(),
        MessageType.INTEGER);
```

Running a allreduction operation on an array of size 8 array with 4 workers iterating once with source parallelism of 8
and sink parallelism of 8, added with result verification. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.comms.ExampleMain -itr 1 -workers 4 -size 8 -op "reduce" -stages 8,8 -verify
```

[Communication based Batch AllReduce Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/comms/batch/BAllReduceExample.java)



### Gather Example

In the gather communication, what happens is the data distributed to multiple points are being
gathered to a single point. 

The communication defining section is as follows,

```bash
  gather = new BGather(communicator, taskPlan, sources, target,
         MessageType.INTEGER, new FinalReduceReceiver(), false);
```

Running a gather operation on an array of size 8 with 4 workers iterating once with source parallelism of 8
and sink parallelism of 1, added with result verification. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.comms.ExampleMain -itr 1 -workers 4 -size 8 -op "gather" -stages 8,1 -verify
```

[Communication based Batch Gather Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/comms/batch/BGatherExample.java)



### AllGather Example

Allgather communication, what happens is the data distributed to multiple points are being
gathered to all the points. So each point will have the data owned by the other points as well. 

The communication defining section is as follows,

```bash
  gather = new BAllGather(communicator, taskPlan, sources, targets, new FinalSingularReceiver(),
          MessageType.INTEGER);
```

Running a gather operation on an array of size 8 with 4 workers iterating once with source parallelism of 8
and sink parallelism of 8, added with result verification. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.comms.ExampleMain -itr 1 -workers 4 -size 8 -op "allgather" -stages 8,8 -verify
```

[Communication based Batch AllGather Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/comms/batch/BAllGatherExample.java)


### Partition Example

In Partition communication, what happens is the data can be distributed to multiple points. So each point will have the data owned by the other points as well. 

The communication defining section is as follows,

```bash
  partition = new BPartition(communicator, taskPlan, sources, targets,
          MessageType.INTEGER, new PartitionReceiver(), new LoadBalanceSelector(), false);
```

The partition logic can be selected by the user. In this example, the LoadBalance selector is being
used. 

Running a gather operation on an array of size 8 with 4 workers iterating once with source parallelism of 8
and sink parallelism of 8, added with result verification. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.comms.ExampleMain -itr 1 -workers 4 -size 8 -op "partition" -stages 8,8 -verify
```

[Communication based Batch Partition Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/comms/batch/BPartitionExample.java)

### KeyedReduce Example

In KeyedReduce communication, the logic is same as the basic reduce operation, but when the data is being keyed
based on a given criteria, the keyed-reduce communication can be used to perform the reduction operation.  

The communication defining section is as follows,

```bash
   keyedReduce = new BKeyedReduce(communicator, taskPlan, sources, targets,
          new ReduceOperationFunction(Op.SUM, MessageType.INTEGER),
          new FinalBulkReceiver(), MessageType.INTEGER, MessageType.INTEGER,
          new SimpleKeyBasedSelector());
```

The key selection logic used in this one is the SimpleKeyBasedSelector.  

Running a reduction operation on an array of size 8 with 4 workers iterating once with source parallelism of 8
and sink parallelism of 1, added with result verification. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.comms.ExampleMain -itr 1 -workers 4 -size 8 -op "keyedreduce" -stages 8,1 -verify
```

[Communication based Batch Keyed-Reduce Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/comms/batch/BKeyedReduceExample.java)


### KeyedGather Example

In Keyed-Gather communication, the logic is same as the basic gather operation, but when the data is being keyed
based on a given criteria, the keyed-gather communication can be used to perform the gather operation.  

The communication defining section is as follows,

```bash
       keyedGather = new BKeyedGather(communicator, taskPlan, sources, targets,
           MessageType.INTEGER, MessageType.INTEGER, new FinalReduceReceiver(),
           new SimpleKeyBasedSelector());
```

The key selection logic used in this one is the SimpleKeyBasedSelector.  

Running a reduction operation on an array of size 8 with 4 workers iterating once with source parallelism of 8
and sink parallelism of 1, added with result verification. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.comms.ExampleMain -itr 1 -workers 4 -size 8 -op "keyedgather" -stages 8,1 -verify
```

[Communication based Batch Keyed-Gather Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/comms/batch/BKeyedGatherExample.java)


### KeyedPartition Example

In Keyed-Partition communication, the logic is same as the basic partition operation, but when the data is being keyed
based on a given criteria, the keyed-partition communication can be used to perform the partition operation.  

The communication defining section is as follows,

```bash
   partition = new BKeyedPartition(communicator, taskPlan, sources, targets, MessageType.INTEGER,
        MessageType.INTEGER, new PartitionReceiver(), new SimpleKeyBasedSelector());
```

The key selection logic used in this one is the SimpleKeyBasedSelector.  

Running a reduction operation on an array of size 8 with 4 workers iterating once with source parallelism of 8
and sink parallelism of 8, added with result verification. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.comms.ExampleMain -itr 1 -workers 4 -size 8 -op "keyedpartition" -stages 8,8 -verify
```

[Communication based Batch Keyed-Partition Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/comms/batch/BKeyedPartitionExample.java)


## Streaming Examples


To run the stream examples for each of the collective communication model, please use the -stream
tag. 

#### Example

Running a reduce streaming example using 4 workers, with a single iteration, source parallelism
as 8 and sink parallelism as one. 

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.comms.ExampleMain -itr 1 -workers 4 -size 8 -op "reduce" -stages 8,1 -verify -stream

```

You can use the same running commands used for batch examples, but use the -stream tag.






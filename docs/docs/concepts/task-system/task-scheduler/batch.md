# Batch Schedulers

## Round Robin Batch Task Scheduler

RoundRobinBatchTaskScheduler allocates the task instances of the task graph in a round robin fashion 
which is similar to the Round Robin Task Scheduler for batch tasks. However, the primary difference 
between the streaming and batch task scheduling is, the streaming tasks has been considered as a 
complete task graph whereas the taskgraph for batch tasks has been divided into batches based on the 
level and the dependency of the tasks in the taskgraph. The sample batch taskgraph example is given 
below.

```text
      Source (Task 1)
       |
       |
       V
    Task 2 (Two Outgoing Edges)
    |     |
    |     |
    V     V
  Task 3  Task 4
       |
       |
       V
     Target (Task 5)
```

For the above task graph example, the tasks are divided into the following batches and scheduled into 
the available workers as given below:

```text
**Schedule Batches**

1st batch --> Task 1 (Source)
2nd batch --> Task 2
3rd batch --> Task 3 & Task 4
4th batch --> Task 5 (Target)
```

For example, if there are 2 containers and 4 batches of tasks \(dependency tasks\) with a task 
parallelism value of 2, in the first batch, task instance 0 of 1st task \(Task 1\) will go to 
container 0 and task instance 1 of 1st task will go to container 1. In the second batch, task instance 
0 of 2nd task will go to container 0 and task instance 1 of 2nd will go to container 1. In the third 
batch, task instance 0 of 3rd task will go to container 0, task instance 1 of 3rd task will go to 
container 1, task instance 0 of 4th task will go to container 0 and task instance 1 of 4th task will 
go to container 1. In the fourth batch, task instance 0 of 5th task will go to container 0, task 
instance 1 of 5th task will go to container 1.  

It generates the task schedule plan which consists of multiple containers \(container plan\) and the 
allocation of task instances \(task instance plan\) on those containers. The size of the container 
\(memory, disk, and cpu\) and the task instances \(memory, disk, and cpu\) are homogeneous in nature.
First, it will allocate the task instances into the logical container values and then it will calculate 
the required ram, disk, and cpu values for the task instances and the logical containers which is based 
on the task configuration values and the allocated worker values respectively.

The algorithm first parses the task vertex set of the task graph and identify the source, parent, 
child, and target tasks and store the identified batch of tasks in a separate set. Next, it allocates 
the logical container size based on the default ram, disk, and cpu values specified in the TaskScheduler 
Context. The schedule method unwraps the roundrobincontainer instance map and finds out the task 
instances allocated to each container. Based on the required ram, disk, and cpu of the required task 
instances it creates the required container object. If the worker has required ram, disk, and cpu value 
then it assigns those values to the containers otherwise, it will assign the calculated value of 
required ram, disk, and cpu value to the containers. Finally, the schedule method pack the task 
instance plan and the container plan into the task schedule plan and return the same.

[Round Robin Batch Task Scheduler Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/taskscheduler/src/java/edu/iu/dsc/tws/tsched/batch/roundrobin/RoundRobinBatchTaskScheduler.java)


## Data Locality Batch Task Scheduler

DataLocality Aware Task Scheduler allocates the task instances of the streaming task graph based on 
the locality of data. It calculates the distance between the worker nodes and the data nodes and 
allocate the batch task instances to the worker nodes which are closer to the data nodes i.e. it 
takes lesser time to transfer/access the input data file. The data transfer time is calculated based 
on the network parameters such as bandwidth, latency, and size of the input file. It generates the 
task schedule plan which consists of the containers \(container plan\) and the allocation of task 
instances \(task instance plan\) on those containers. The size of the container \(memory, disk, and cpu\) 
and the task instances \(memory, disk, and cpu\) are homogeneous in nature. First, it computes the 
distance between the worker node and the datanodes and allocate the task instances into the logical 
container values and then it will calculate the required ram, disk, and cpu values for the task 
instances and the logical containers which is based on the task configuration values and the 
allocated worker values respectively.

The algorithm first calculate the total number of task instances could be allocated to the container.
Next, the algorithm retrieve the total number of task instances from the Task Attributes for the 
particular task. Based on the max TaskInstancesPerContainer value, the algorithm allocates the task 
instances into the respective container. 

The algorithm send the task vertex and the distance calculation map to find out the best worker node 
which is calculated between the worker nodes and the data nodes and store it in the map. Then, it 
allocate the task instances of the task vertex to the worker \(which has minimal distance\), 
if the container/worker has reached the maximum number of task instances then it will allocate the 
remaining task instances to the next container. Finally, the algorithm returns the 
datalocalityawareallocation map object which consists of container and its task instance allocation.

The DataLocalityBatchTaskScheduler assign the logical container size which is based on the default 
ram, disk, and cpu values specified in the TaskScheduler Context. Then, the algorithm unwraps the 
datalocalityawarecontainer instance map and finds out the task instances allocated to each container. 
Based on the task instances required ram, disk, and cpu it creates the required container object. 
If the worker has required ram, disk, and cpu value then it assigns those values to the containers 
otherwise, it will assign the calculated value of required ram, disk, and cpu value to the containers. 
Finally, the algorithm pack the task instance plan and the container plan into the task schedule plan 
and return the same.

[Data Locality Batch Task Scheduler Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/taskscheduler/src/java/edu/iu/dsc/tws/tsched/batch/datalocalityaware/DataLocalityBatchTaskScheduler.java)


## Batch Task Scheduler

Batch Task Scheduler is capable of scheduling of single task graph as well as multiple graphs which
depends on the input from other task graphs. If the batch task scheduler receives only single task
graph, first it will check whether it has any receptor and collector tasks in the graph. If the receptor
and collector task doesn't match the parallelism it throws the runtime exception to the user to provide
the same parallelism for the dependent tasks in the graph. If the batch task scheduler receives multiple
task graph, first it will store the collectible name set and receivable name set in the appropriate 
set values. If the collectible name set (input key) matches with the receivable name set (input key) 
it validate the parallelism of the dependent tasks in the task graph, if it doesn't match it will 
guide the user to specify the same parallelism. If it matches, it proceeds with the scheduling of the 
task graphs to the same workers. For example, if the map task, 0th task has the data in worker 0, it 
will schedule the reduce task 0th task to the worker 1. Batch Task Scheduler considers both the locality 
of the data and scheduling the tasks in a round robin fashion. 

[Batch Task Scheduler Source Code](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/taskscheduler/src/java/edu/iu/dsc/tws/tsched/batch/batchscheduler/BatchTaskScheduler.java)
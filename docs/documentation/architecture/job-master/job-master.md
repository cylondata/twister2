Job Master Design, Features and Usage
=======================================

The Job Master manages job related activities during job execution such as 
worker life cycle management, worker discovery, resource cleanup, 
dynamic resource allocation, fault tolerance, etc.
 
Currently, we implemented the following services:
1. Ping Service
2. Worker life-cycle monitoring 
3. Worker Discovery
4. Job Termination
5. Barrier Service

## Possibilities for Job Master Architecture 
There are three architectural alternatives for the design of the Job Master. 
1. **Long Running Singleton Job Master**: A single Job master process may serve all Twister2 jobs in a cluster.
It runs as a a long running service in a dedicated machine on the cluster. 
All jobs can be tracked by this single Job manager.
   * A disadvantage of this solution is that it puts too much pressure on a single process to manage all jobs.
2. **Job submission client becomes the Job master**: When a user submits a Twister2 job, 
its process continues to run and become the job master. It means each job will have 
a different job master.
   * One disadvantage might be that the job submitting client can not run outside of the cluster, 
since all workers need to connect to the Job Master. Submitting client has to run in one of 
the cluster machines. 
   * Another disadvantage is that this solution is not really suitable for long running jobs. 
3. **A separate Job Master for each Job**: Submitting client instantiates a separate Job master 
for each job on a cluster machine. 
   * This may increase the job initialization times, since the job master needs to be started as a separate entity for each job. 
   * A separate entity is introduced to the system. This increases the overall complexity of Twister2 ecosystem. 

## Job Master in Other Systems
**Heron**: Option 3 is implemented. One topology master is initiated for each job. 

**Hadoop MapReduce v2**: Option 3 is implemented. One ApplicationMaster is initiated for each job. 

**Spark**: Option 2 is implemented. SparkContext is initialized in the job submitting client. 
It acts as the job master. 

**Flink**: Option 1 is implemented. A single long running Job Manager schedules all jobs in a cluster. 
One Job Manager instance runs in one cluster. 

## Twister2 Job Master Architecture
We decided to choose the option 3 for the Job Master architecture. 
A separate Job master is started for each job. It runs in a cluster node. 
It is started when a job is submitted and deleted after the job has completed. 

We also support the option 2. Submitting client can become the Job Master. 
With that option, our solution covers both the first and the second architectural options.

By default, Job Master is started as a separate process. If the user wants to start it
in the submitting client, the value of the following configuration parameter needs to be set as true. 
* twister2.job.master.runs.in.client

## Workers to Job Master Communications
We use protocol buffers to exchange messages between workers and the job master. 
We transfer the messages over TCP. 
All messages are in request-response semantics. All communications are initiated by workers. 
The job master only responds to requests. 

## Job Master Threading
Job Master is developed as a single threaded application.
When a request message is received, a single thread is waken up. 
It processes the message and sends the response if needed. 
Then it starts to sleep and wait for the next message. 
It always starts to execute by worker request messages. 

## Ping Service
When a worker starts, after it sends the worker STARTING message, it starts sending ping messages. 
Main client thread sends ping messages to the job master periodically. 
As long as the job master gets ping messages from a worker, it assumes that the worker is healthy. 
The job master gets the ping message, saves the ping time and sends a response message back to the worker. 

Ping Message Format is shown below. It has the workerID from the sender worker. 
It also has the ping message and an optional string message for logging purposes. 

    message Ping {
        oneof required {
            int32 workerID = 1;
        }
        string pingMessage = 2;
        MessageType messageType = 3;
    
        enum MessageType {
            WORKER_TO_MASTER = 0;
            MASTER_TO_WORKER = 1;
        }
    }
  
Users can set the ping message sending intervals through configuration files. 
A default value is specified in the code, if they chose to not set it. 
Configuration parameter name is: twister2.worker.ping.interval

Ping service is started automatically. Users do not need to start it. 

## Worker Life Cycle Monitoring
Each worker reports its state changes to the job master during the job execution. 
Currently we have the following states for workers: 

    enum WorkerState {
      STARTING = 0;
      RUNNING = 1;
      COMPLETED = 2;
      ERROR = 3;
      UNASSIGNED = 4;
    }

When a worker is in the initializing phase, it first sends a STARTING message. 
It sends its IP address and its port number in this message. STARTING message
registers the worker with the Job Master. STARTING message is mandatory. 
It can not be omitted. After a request is received for the STARTING message,
the worker can send other messages. 

After the worker completes initialization and ready to execute the user code, 
it sends RUNNING message. When it has completed the computation, 
it sends COMPLETED message. 

UNASSIGNED state is used in the program code to handle the initial state 
when no state information is present for a worker. 
UNASSIGNED message is not exchanged between the worker and the job master.

ERROR message is not currently used. We plan to use it to report error cases 
in the future. 

Workers send WorkerStateChange message to the job master to inform it 
about its state change. The format of the message is as follows:

    message WorkerStateChange {
        WorkerNetworkInfo workerNetworkInfo = 1;
        WorkerState newState = 2;
    }

WorkerNetworkInfo message defines the networking properties of a Twister2 worker:

    message WorkerNetworkInfo {
        oneof required {
            int32 workerID = 1;
        }
        string workerIP = 2;
        int32 port = 3;
        string nodeIP = 4;
        string rackName = 5;
        string dataCenterName = 6;
    }

The job master in response sends the following message to the worker. 

    message WorkerStateChangeResponse {
        oneof required {
            int32 workerID = 1;
        }
        WorkerState sentState = 2;
    }

## Worker Discovery
Job Master provides worker discovery service to workers in a job. 
When a worker starts initializing, it sends its IP address and the port number 
to the job master. Job master keeps the list of all workers in a job 
with their network address information.
 
Workers send ListWorkersRequest message to get the list of all workers in a job 
with the network information. The message proto is shown below. 
Workers can get either the current list joined workers from the job master, 
or they can request the full list. In that case, the full list will be sent 
when all workers joined the job. When they ask the current list, 
they get the list of joined workers immediately. 
In both cases, this list includes the workers that have already left if any. 

    message ListWorkersRequest {
        enum RequestType {
            IMMEDIATE_RESPONSE = 0;
            RESPONSE_AFTER_ALL_JOINED = 1;
        }
    
        oneof required {
            int32 workerID = 1;
        }
        RequestType requestType = 2;
    }

Job master sends the worker list in the following message format. 
It sends many worker information on the same message. 

    message ListWorkersResponse {
    
        oneof required {
            int32 workerID = 1;
        }
        repeated WorkerNetworkInfo workers = 2;
    }

**IWorkerController Implementation**  
We developed the WorkerController class that will be used by the workers 
to interact with the job master. The class name is:

    edu.iu.dsc.tws.master.client.JMWorkerController

It implements the interface: 

    edu.iu.dsc.tws.common.discovery.IWorkerController

All worker implementations can utilize this class for worker discovery. 

### WorkerID Assignment
Each worker in a job should have a unique ID. Worker IDs start from 0 and 
increase sequentially without any gaps in between. 
We support two types of workerID assignments. 
The first option is that the Job Master assigns worker IDs. 
It assigns workerIDs in the order of their registration with the Job Master 
with worker STARTING message. It assigns the id 0 to the first worker to be registered. 
It assigns the id 1 to the second worker to be registered, so on. 

The second option for the worker ID assignment is that the underlying Twister2 implementation 
may assign unique IDs for the workers. In this case, when workers register 
with the Job Master, they already have a valid unique ID. 
So, the Job Master does not assign a new ID to them. It uses their IDs. 

Worker ID assignment method is controlled by a configuration parameter. 
The configuration parameter name is: 

    twister2.job.master.assigns.worker.ids

If its value is true, Job Master assigns the worker IDs. 
If its value is false, underlying resource scheduler assigns worker IDs. 
By default, its value is true. 

### Waiting Workers on a Barrier
Job Master implements the barrier mechanism by using a waitList. 
Each worker that comes to the barrier point sends the following BarrierRequest message 
to the Job Master. 

    message BarrierRequest {
        oneof required{
            int32 workerID = 1;
        }
    }

Job Master puts the received request messages to the waitList.
When it receives the BarrierRequest message from the last worker in the job, 
it sends the following response message to all workers: 

    message BarrierResponse {
        oneof required {
            int32 workerID = 1;
        }
    }

When it sends the response messages to workers, it clears the waitList.
Therefore, it can start a new barrier after all workers are released.

## Job Termination
Job termination is handled differently in different cluster management systems 
such as Kubernetes and Mesos. Therefore, we designed an Interface 
to terminate jobs. This interface needs to be implemented on 
the relevant cluster management system. Then, an instance of this interface 
will be provided to the Job Master when it is initialized. 
Job Master calls the terminate method for a job after it receives 
worker COMPLETED messages from all workers in that job.
  
Job termination interface.
  
    package edu.iu.dsc.tws.master;
    
    public interface IJobTerminator {
      boolean terminateJob(String jobName);
    }

An implementation of IJobTerminator for Kubernetes clusters can be found in:
* edu.iu.dsc.tws.rsched.schedulers.k8s.master.JobTerminator

## Usage

Job Master has two components: 

    edu.iu.dsc.tws.master.JobMaster
    edu.iu.dsc.tws.master.client.JobMasterClient
 
JobMaster is the server that needs to be started for each job. 
JobMasterClient is the client that runs in workers and interacts with the server. 

### JobMaster Usage 
JobMaster may be started locally to use in development testing. 
JobMaster needs to know the port number to serve, number of workers in the job, job name,
a Job Terminator, etc. With these data, it can be started locally. 
If there is no need for Job Terminator, that may be null. 

A sample usage is provided in the example class: 

    edu.iu.dsc.tws.examples.internal.jobmaster.JobMasterExample 

When Jobs are executed in resource schedulers such as in Kubernetes and Mesos, 
Job Master needs to be deployed in those systems. It is usually a good practice to write
a separate JobMasterStarter class to start the job master in those environments. 
This starter class sets up the environment for the Job Master and starts it. 

An example JobMasterStarter class is provided for Kubernetes cluster:

    edu.iu.dsc.tws.rsched.schedulers.k8s.master.JobMasterStarter

It gets the required configuration parameters as environment variables and 
sets up the persistent storage and logging. Then, It starts the JobMaster. 

### JobMasterClient Usage 

JobMasterClient class is used to send all supported messages to JobMaster server and 
receive the responses. 

It is a single threaded class. Since it needs to periodically send Ping messages, 
it is developed as a Threaded application. 

Some JobMasterClient methods are blocking calls. They wait until the response is received.
Some other methods are non-blocking sends. They send the message, but do not wait for the
response to proceed. 

A sample usage is provided in the example class: 

    edu.iu.dsc.tws.examples.internal.jobmaster.JobMasterClientExample 

### IWorkerController Usage
 
JobMasterClient provides an implementation of IWorkerController interface. 
It is automatically initialized when a JobMasterClient is initialized. 
It can be accessed by the method: 

    public WorkerController getWorkerController()

A sample development usage of this client is given in the example class of JobMasterClientExample.
Its usage in Kubernetes environments are provided in classes: 

    edu.iu.dsc.tws.rsched.schedulers.k8s.worker.K8sWorkerStarter
    edu.iu.dsc.tws.rsched.schedulers.k8s.mpi.MPIWorkerStarter

## Configuration Parameters
Job Master related configuraion parameters are handled by the class:

    edu.iu.dsc.tws.master.JobMasterContext

Users can specify the following configuration parameters. 
Their names are default values are shown:

    twister2.job.master.assigns.worker.ids: true
    twister2.job.master.runs.in.client: false
    twister2.job.master.cpu: 0.2
    twister2.job.master.ram: 1024
    twister2.job.master.volatile.volume.size: 1.0
    twister2.job.master.persistent.volume.size: 1.0
    twister2.job.master.port: 11011
    twister2.worker.ping.interval: 10000
    twister2.worker.to.job.master.response.wait.duration: 10000


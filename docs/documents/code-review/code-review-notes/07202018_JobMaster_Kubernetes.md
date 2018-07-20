
# Job Master Code Review [Draft:Check grammar and order of the content]

Job master is a single threaded app. 

## Doubts
ListWorkerRequest: immediate message sending ( real use case????)
WorkerMonitor uses this clause??? (What is the purpose of the immediate message in this one)
ListWorkersMessageReceived[method]

Usage for WebUI to tell how many nodes are online. 

## Job Master 

Move the main classes in the Kubernetes work to test cases.

The JobMaster needs to be implemented with Runnable so that later we can create an instance and start a spearate thread for the job master. Because the caller has the control over the control of the thread. 

Util method starting a thread for JobMaster. Keeping JobMaster independent of threading and use the Util to wrap it in a thread. 


Worker monitor recieves completed messages from all workers and mark all as completed. 

For the server, a function must be added like wait until all the messages are receieved. 

All the messages being consumed can be tracked by ???

A mechanism to track all the messages were received. 

If the messages sent from Workers to Job Master is received a message is sent by the Master to Worker saying the message is received. If the message get lost, will the worker send a message again. This is a point that has to be checked. 

Worker start first or JobMaster start first. (doubtful)

OnConnect in JobMasterClient, a queue must be maintained to know whether an error was recorded or not.

Job Master port dynamical allocation, a way must be there to discover that port. Zookeeper for Kubernetes port discovery (as Mesos section in the project has used it)

## Logging Format

Log.info must be replaced with Log.log(Level.INFO) for the purpose of consistency. 

sendRequestWaitResponse : logging or throw an exception in the case of wait limit has been reached when the messages are not receie. 

## Project Specific Exceptions

Handle exceptions in the project with unique Exception classes for the different parts of the project. 

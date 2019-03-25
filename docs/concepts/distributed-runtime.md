# Distributed Runtime

Each Twister2 job runs in isolation without any interference from other jobs. Because of this 
the distributed runtime of Twister2 is relevant to each job.

At the bottom of the Twister2 is the data access APIs. These APIs provide mechanisms to access data
in file systems, message brokers and databases. On top of these 

![Twister2 Runtime](../images/runtime.png)

Twister2 spawns several processes when running a job. 

1. Driver - The program initiating the job
2. Worker Process - A process spawn by resource manager for running the user code. 
3. Job Master - One job master for each job
4. Dashboard - A web service hosting the UI 

![Twister2 Process View](../images/process_view.png)



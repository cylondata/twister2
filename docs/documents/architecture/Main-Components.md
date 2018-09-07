# Main Components of Twister2 Architecture

Twister2 Runtime consists of the following main components.

1. Job Submission Client
2. Job Master
3. Workers

### Job Submission Client
This is the program that the user use to submit/terminate/modify Twister2 jobs. 
It may run in the cluster or outside of it in the user machine. 

### Job Master
Job Master manages the job related activities during job execution 
such as fault tolerance, life-cycle management, dynamic resource allocation, 
resource cleanup, etc. 

### Workers
The processes that perform the computations in a job.

### Twister2 Web UI (To Be Added): 
This will present the job related data to users through a web page. 
Users will be able to monitor their jobs on that web page. 
Only one instance will run in the cluster and it will provide 
data for all jobs running in the cluster. 

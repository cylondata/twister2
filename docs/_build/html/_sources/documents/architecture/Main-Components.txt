# Main Components of Twister2 Architecture
Ahmet Uyar

Twister2 have the following main components:
* **Workers**: The processes that perform the computations in a job.
* **Job Master**: Job Master manages the job related activities during job execution 
such as fault tolerance, life-cycle management, dynamic resource allocation, 
resource cleanup, etc. 
* **Twister2 Clients**: This is the program that the users use to submits/terminates/modifies 
Twister2 jobs. It may run in the cluster or outside of it. 
* **Twister2 Web UI**: This will provide the job related data to users. 
Users will be able to monitor their job on that web page. 
Only one instance will run in the cluster. This Web UI will provide 
data for all jobs running in the cluster. 


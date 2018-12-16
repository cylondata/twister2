# Kubernetes System Configuration



### <br/>Logging related settings<br/>for Twister2 workers
**twister2.logging.level**
<table><tr><td>default</td><td>"INFO"</td><tr><td>description</td><td><br/>default value is INFO</td></table>

**persistent.logging.requested**
<table><tr><td>default</td><td>true</td><tr><td>description</td><td><br/>Do workers request persistent logging? it could be true or false<br/>default value is false</td></table>

**twister2.logging.redirect.sysouterr**
<table><tr><td>default</td><td>false</td><tr><td>description</td><td><br/>whether System.out and System.err should be redircted to log files<br/>When System.out and System.err are redirected to log file, <br/>All messages are only saved in log files. Only a few intial messages are shown on Dashboard.<br/>Otherwise, Dashboard has the complete messages,<br/>log files has the log messages except System.out and System.err.</td></table>

**twister2.logging.max.file.size.mb**
<table><tr><td>default</td><td>100</td><tr><td>description</td><td><br/>The maximum log file size in MB</td></table>

**twister2.logging.maximum.files**
<table><tr><td>default</td><td>5</td><tr><td>description</td><td><br/>The maximum number of log files for each worker</td></table>

### <br/>Twister2 Job Master related settings
**twister2.job.master.runs.in.client**
<table><tr><td>default</td><td>false</td><tr><td>description</td><td><br/>if true, the job master runs in the submitting client<br/>if false, job master runs as a separate process in the cluster <br/>by default, it is true<br/>when the job master runs in the submitting client,<br/>this client has to be submitting the job from a machine in the cluster<br/>getLocalHost must return a reachable IP address to the job master</td></table>

**twister2.job.master.assigns.worker.ids**
<table><tr><td>default</td><td>false</td><tr><td>description</td><td><br/>if true, job master assigns the worker IDs,<br/>if false, workers have their IDs when regitering with the job master</td></table>

**twister2.worker.ping.interval**
<table><tr><td>default</td><td>10000</td><tr><td>description</td><td><br/>ping message intervals from workers to the job master in milliseconds<br/>default value is 10seconds = 10000</td></table>

**twister2.job.master.port**
<table><tr><td>default</td><td>11011</td><tr><td>description</td><td><br/>twister2 job master port number<br/>default value is 11011</td></table>

**twister2.worker.to.job.master.response.wait.duration**
<table><tr><td>default</td><td>10000</td><tr><td>description</td><td><br/>worker to job master response wait time in milliseconds<br/>this is for messages that wait for a response from the job master<br/>default value is 10seconds = 10000</td></table>

**twister2.job.master.volatile.volume.size**
<table><tr><td>default</td><td>0.0</td><tr><td>description</td><td><br/>twister2 job master volatile volume size in GB<br/>default value is 1.0 Gi<br/>if this value is 0, volatile volume is not setup for job master</td></table>

**twister2.job.master.persistent.volume.size**
<table><tr><td>default</td><td>0.0</td><tr><td>description</td><td><br/>twister2 job master persistent volume size in GB<br/>default value is 1.0 Gi<br/>if this value is 0, persistent volume is not setup for job master</td></table>

**twister2.job.master.cpu**
<table><tr><td>default</td><td>0.2</td><tr><td>description</td><td><br/>twister2 job master cpu request<br/>default value is 0.2 percentage</td></table>

**twister2.job.master.ram**
<table><tr><td>default</td><td>1024</td><tr><td>description</td><td><br/>twister2 job master RAM request in MB<br/>default value is 1024 MB</td></table>

### <br/>WorkerController related config parameters
**twister2.worker.controller.max.wait.time.for.all.workers.to.join**
<table><tr><td>default</td><td>100000</td><tr><td>description</td><td><br/>amount of timeout for all workers to join the job<br/>in milli seconds</td></table>

**twister2.worker.controller.max.wait.time.on.barrier**
<table><tr><td>default</td><td>100000</td><tr><td>description</td><td><br/>amount of timeout on barriers for all workers to arrive<br/>in milli seconds</td></table>

### <br/>Dashboard related settings
**twister2.dashboard.host**
<table><tr><td>default</td><td>"http://localhost:8080"</td><tr><td>description</td><td><br/>Dashboard server host address and port<br/>if this parameter is not specified, then job master will not try to connect to Dashboard</td></table>


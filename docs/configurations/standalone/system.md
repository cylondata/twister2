# Standalone System Configuration



###  Logging related settings for Twister2 workers
**twister2.logging.level**
<table><tr><td>default</td><td>"INFO"</td><tr><td>description</td><td>default value is INFO</td></table>

**persistent.logging.requested**
<table><tr><td>default</td><td>true</td><tr><td>description</td><td>Do workers request persistent logging? it could be true or false default value is false</td></table>

**twister2.logging.redirect.sysouterr**
<table><tr><td>default</td><td>false</td><tr><td>description</td><td>whether System.out and System.err should be redircted to log files When System.out and System.err are redirected to log file, All messages are only saved in log files. Only a few intial messages are shown on Dashboard. Otherwise, Dashboard has the complete messages, log files has the log messages except System.out and System.err.</td></table>

**twister2.logging.max.file.size.mb**
<table><tr><td>default</td><td>100</td><tr><td>description</td><td>The maximum log file size in MB</td></table>

**twister2.logging.maximum.files**
<table><tr><td>default</td><td>5</td><tr><td>description</td><td>The maximum number of log files for each worker</td></table>

**twister2.logging.format: "[%1$tF %1$tT] [%4$s] %3$s**
<table><tr><td>default</td><td>%5$s %6$s %n"</td><tr><td>description</td><td>the java utils log file format to be used</td></table>

###  Twister2 Job Master related settings
**twister2.job.master.used**
<table><tr><td>default</td><td>false</td><tr><td>description</td><td></td></table>

**twister2.job.master.runs.in.client**
<table><tr><td>default</td><td>false</td><tr><td>description</td><td>if true, the job master runs in the submitting client if false, job master runs as a separate process in the cluster by default, it is true when the job master runs in the submitting client, this client has to be submitting the job from a machine in the cluster</td></table>

**twister2.job.master.assigns.worker.ids**
<table><tr><td>default</td><td>false</td><tr><td>description</td><td>if true, job master assigns the worker IDs, if false, workers have their IDs when regitering with the job master</td></table>

###  WorkerController related config parameters
**twister2.worker.controller.max.wait.time.for.all.workers.to.join**
<table><tr><td>default</td><td>100000</td><tr><td>description</td><td>amount of timeout for all workers to join the job in milli seconds</td></table>

**twister2.worker.controller.max.wait.time.on.barrier**
<table><tr><td>default</td><td>100000</td><tr><td>description</td><td>amount of timeout on barriers for all workers to arrive in milli seconds</td></table>

###  Dashboard related settings
**twister2.dashboard.host**
<table><tr><td>default</td><td>"http://localhost:8080"</td><tr><td>description</td><td>Dashboard server host address and port if this parameter is not specified, then job master will not try to connect to Dashboard</td></table>


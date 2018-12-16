# Slurm System Configuration



###  WorkerController related config parameters
**twister2.worker.controller.max.wait.time.for.all.workers.to.join**
<table><tr><td>default</td><td>100000</td><tr><td>description</td><td>amount of timeout for all workers to join the job in milli seconds</td></table>

**twister2.worker.controller.max.wait.time.on.barrier**
<table><tr><td>default</td><td>100000</td><tr><td>description</td><td>amount of timeout on barriers for all workers to arrive in milli seconds</td></table>

###  Dashboard related settings
**twister2.dashboard.host**
<table><tr><td>default</td><td>"http://localhost:8080"</td><tr><td>description</td><td>Dashboard server host address and port if this parameter is not specified, then job master will not try to connect to Dashboard</td></table>


# Standalone Task Configuration



**twister2.streaming.taskscheduler**
<table><tr><td>default</td><td>"roundrobin"</td><tr><td>options</td><td>"firstfit"<br/></td><tr><td>description</td><td> Streaming Task Scheduler Mode "roundrobin" or  "firstfit" or "datalocalityaware"</td></table>

**twister2.streaming.taskscheduler.class**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.tsched.streaming.roundrobin.RoundRobinTaskScheduler"</td><tr><td>options</td><td>"edu.iu.dsc.tws.tsched.streaming.roundrobin.ResourceAwareRoundRobinTaskScheduler"<br/>"edu.iu.dsc.tws.tsched.streaming.firstfit.FirstFitStreamingTaskScheduler"<br/></td><tr><td>description</td><td> Streaming Task Scheduler Class</td></table>

**twister2.batch.taskscheduler**
<table><tr><td>default</td><td>"roundrobin"</td><tr><td>description</td><td></td></table>

**twister2.batch.taskscheduler.class**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.tsched.batch.roundrobin.RoundRobinBatchTaskScheduler"</td><tr><td>description</td><td></td></table>

**twister2.task.instances**
<table><tr><td>default</td><td>2</td><tr><td>description</td><td></td></table>

**twister2.task.instance.ram**
<table><tr><td>default</td><td>512.0</td><tr><td>description</td><td></td></table>

**twister2.task.instance.disk**
<table><tr><td>default</td><td>500.0</td><tr><td>description</td><td></td></table>

**twister2.task.instance.cpu**
<table><tr><td>default</td><td>2.0</td><tr><td>description</td><td></td></table>

**twister2.ram.padding.container**
<table><tr><td>default</td><td>2.0</td><tr><td>description</td><td></td></table>

**twister2.disk.padding.container**
<table><tr><td>default</td><td>12.0</td><tr><td>description</td><td></td></table>

**twister2.cpu.padding.container**
<table><tr><td>default</td><td>1.0</td><tr><td>description</td><td></td></table>

**twister2.container.padding.percentage**
<table><tr><td>default</td><td>2</td><tr><td>description</td><td></td></table>

**twister2.container.instance.ram**
<table><tr><td>default</td><td>4096.0</td><tr><td>description</td><td></td></table>

**twister2.container.instance.disk**
<table><tr><td>default</td><td>8000.0</td><tr><td>description</td><td></td></table>

**twister2.container.instance.cpu**
<table><tr><td>default</td><td>16.0</td><tr><td>description</td><td></td></table>

**twister2.container.instance.bandwidth**
<table><tr><td>default</td><td>10 #Mbps</td><tr><td>description</td><td></td></table>

**twister2.container.instance.latency**
<table><tr><td>default</td><td>0.002 #Milliseconds</td><tr><td>description</td><td></td></table>

**twister2.datanode.instance.bandwidth**
<table><tr><td>default</td><td>20 #Mbps</td><tr><td>description</td><td></td></table>

**twister2.datanode.instance.latency**
<table><tr><td>default</td><td>0.01 #Milliseconds</td><tr><td>description</td><td></td></table>

**twister2.task.parallelism**
<table><tr><td>default</td><td>2</td><tr><td>description</td><td></td></table>

**twister2.task.type**
<table><tr><td>default</td><td>"streaming"</td><tr><td>description</td><td></td></table>


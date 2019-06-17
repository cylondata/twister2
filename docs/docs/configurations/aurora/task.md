# Aurora Task Configuration

**twister2.taskscheduler.streaming**
<table><tr><td>default</td><td>"roundrobin"</td><tr><td>options</td><td>"firstfit", "datalocalityaware", "userdefined"</td>
<tr><td>description</td><td>It is the configuration for the streaming task scheduling mode</td></table>

**twister2.taskscheduler.streaming.class**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.tsched.streaming.roundrobin.RoundRobinTaskScheduler"</td><tr><td>options</td>
<td>"edu.iu.dsc.tws.tsched.streaming.firstfit.FirstFitStreamingTaskScheduler", 
"edu.iu.dsc.tws.tsched.streaming.datalocalityaware.DataLocalityStreamingTaskScheduler", 
"edu.iu.dsc.tws.tsched.userdefined.UserDefinedTaskScheduler" </td><tr><td>description</td>
<td>It is the streaming task scheduler class for the respective task scheduling mode</td></table>

**twister2.taskscheduler.batch**
<table><tr><td>default</td><td>"roundrobin"</td><tr><td>options</td><td>"datalocalityaware", "userdefined"</td>
<tr><td>description</td><td>It is the configuration for the batch task scheduling mode</td></table>

**twister2.taskscheduler.batch.class**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.tsched.batch.roundrobin.RoundRobinBatchTaskScheduler"</td>
<tr><td>options</td><td>"edu.iu.dsc.tws.tsched.batch.datalocalityaware.DataLocalityBatchTaskScheduler", 
"edu.iu.dsc.tws.tsched.userdefined.UserDefinedTaskScheduler"</td><tr><td>description</td>
<td>It is the batch task scheduler class for the respective task scheduling mode</td></table>

**twister2.taskscheduler.task.instances**
<table><tr><td>default</td><td>2</td><tr><td>description</td><td>Default task instances</td></table>

**twister2.taskscheduler.task.instance.ram**
<table><tr><td>default</td><td>512.0</td><tr><td>description</td><td>Default task instance ram</td></table>

**twister2.taskscheduler.task.instance.disk**
<table><tr><td>default</td><td>500.0</td><tr><td>description</td><td>Default task instance disk</td></table>

**twister2.taskscheduler.task.instance.cpu**
<table><tr><td>default</td><td>2.0</td><tr><td>description</td><td>Default task instance cpu</td></table>

**twister2.taskscheduler.ram.padding.container**
<table><tr><td>default</td><td>2.0</td><tr><td>description</td><td>Default ram container padding</td></table>

**twister2.taskscheduler.disk.padding.container**
<table><tr><td>default</td><td>12.0</td><tr><td>description</td><td>Default disk container padding</td></table>

**twister2.taskscheduler.cpu.padding.container**
<table><tr><td>default</td><td>1.0</td><tr><td>description</td><td>Default cpu container padding</td></table>

**twister2.taskscheduler.container.padding.percentage**
<table><tr><td>default</td><td>2</td><tr><td>description</td><td>Default padding percentage</td></table>

**twister2.taskscheduler.container.instance.ram**
<table><tr><td>default</td><td>4096.0</td><tr><td>description</td><td>Default container ram</td></table>

**twister2.taskscheduler.container.instance.disk**
<table><tr><td>default</td><td>8000.0</td><tr><td>description</td><td>Default container disk</td></table>

**twister2.taskscheduler.container.instance.cpu**
<table><tr><td>default</td><td>16.0</td><tr><td>description</td><td>Default container cpu</td></table>

**twister2.taskscheduler.container.instance.bandwidth**
<table><tr><td>default</td><td>10 #Mbps</td><tr><td>description</td><td>Default container bandwidth</td></table>

**twister2.taskscheduler.container.instance.latency**
<table><tr><td>default</td><td>0.002 #Milliseconds</td><tr><td>description</td><td>Default container latency</td></table>

**twister2.taskscheduler.datanode.instance.bandwidth**
<table><tr><td>default</td><td>20 #Mbps</td><tr><td>description</td><td>Default datanode bandwidth</td></table>

**twister2.taskscheduler.datanode.instance.latency**
<table><tr><td>default</td><td>0.01 #Milliseconds</td><tr><td>description</td><td>Default datanode latency</td></table>

**twister2.taskscheduler.task.parallelism**
<table><tr><td>default</td><td>2</td><tr><td>description</td><td>Default task parallelism</td></table>

**twister2.taskscheduler.task.type**
<table><tr><td>default</td><td>"streaming"</td><tr><td>description</td><td>Default task type</td></table>


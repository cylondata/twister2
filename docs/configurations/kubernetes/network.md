# Kubernetes Network Configuration



**twister2.network.channel.class**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.comms.dfw.tcp.TWSTCPChannel"</td><tr><td>description</td><td></td></table>

### <br/>OpenMPI settings<br/>A Secret object must be present in Kubernetes master<br/>Its name must be specified here
### <br/>Worker port settings<br/>the base port number workers will use internally to communicate with each other<br/>when there are multiple workers in a pod, first worker will get this port number,<br/>second worker will get the next port, and so on.<br/>default value is 9000,
**kubernetes.worker.transport.protocol**
<table><tr><td>default</td><td>"TCP"</td><tr><td>description</td><td><br/>transport protocol for the worker. TCP or UDP<br/>by default, it is TCP<br/>set if it is UDP</td></table>

### <br/>NodePort service parameters
**kubernetes.node.port.service.requested**
<table><tr><td>default</td><td>true</td><tr><td>description</td><td><br/>if the job requests NodePort service, it must be true<br/>NodePort service makes the workers accessibale from external entities (outside of the cluster)<br/>by default, its value is false</td></table>

**by default Kubernetes uses the range**
<table><tr><td>default</td><td>30000-32767 for NodePorts</td><tr><td>options</td><td>30003</td><tr><td>description</td><td><br/>if NodePort value is 0, it is automatically assigned a value<br/>the user can request a specific port value in the NodePort range by setting the value below<br/>Kubernetes admins can change this range</td></table>

**network.send.pending.max**
<table><tr><td>default</td><td>4</td><tr><td>description</td><td><br/>the buffer size to be used<br/>number of send buffers to be used<br/>number of receive buffers to be used<br/>channel pending messages<br/>the send pending messages</td></table>

**network.partition.message.group.high_water_mark**
<table><tr><td>default</td><td>16</td><tr><td>description</td><td><br/>group up to 8 ~ 16 messages<br/>this is the max number of messages to group</td></table>

**shuffle.memory.records.max**
<table><tr><td>default</td><td>10240</td><tr><td>description</td><td><br/>the maximum amount of bytes kept in memory for operations that goes to disk<br/>the maximum number of records kept in memory for operations that goes to dist</td></table>


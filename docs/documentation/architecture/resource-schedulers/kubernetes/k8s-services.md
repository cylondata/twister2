Services for Twister2 Jobs on Kubernetes
========================================

We start a headless service for a regular Twister2 job. 
Those jobs do not expose any ports to external entities. 
However, each worker has its own port and it can communicate with other workers 
in the same job through that port.

When a Twister2 job wants to expose some of its ports to external access, 
we use NodePort feature of Kubernetes system. Each worker exposes one port to outside access. 
Outside entities send requests to the address: <nodeIP>:<NodePort>. 
Therefore, at least some of Kubernetes nodes must have public IP addresses.  
In addition, the port on NodePort must be accessible from outside. 
 
When there are multiple nodes in a cluster, if all those nodes have public IP addresses, 
NodePort service can be reached by using any one of those node IP addresses. 
It does not matter which node IP address the requests are sent to. 
All requests to all node IP addresses on NodePort are directed to one of the workers. 

When  there are multiple workers in a Twister2 job, the incoming requests are directed 
to workers by Kubernetes. Each request is directed to one worker. 
Outside entities does not know which worker they will connect to when they send the request. 
Kubernetes manages requests to worker mapping.  

## Configuration Parameters
Two configuration parameters are added for NodePort service. 

**Enabling NodePort Service**: One parameter shows whether a NodePort service is requested 
for the submitted job. Its value is false by default. When requesting a NodePort service, 
its value has to be “true”.

    kubernetes.node.port.service.requested

**NodePort Service Port Number**: This parameter determines the port value for NodePort service. 
If the user does not set this parameter, then NodePort value is dynamically assigned. 
The user can learn the dynamically assigned NodePort value by querying Kubernetes master 
with the command “kubectl get services -o wide”. 

    kubernetes.service.node.port

## Limitations
NodePort supports only a single port on each pod. Therefore, when using NodePort service, 
number of workers per pod has to be 1. We can not run more than one worker on a pod. 
Because, Kubernetes does not direct requests to more than one port on a pod.
  
I tried assigning names to container ports and set the name as targetPort in NodePort service object, 
but that does not work. It seems that currently Kubernetes does not direct requests 
to more than one port on a pod.

## Other Alternatives: Future Works
Another alternative for exposing services with Kubernetes is to use LoadBalancers. 
We have not implemented LoadBalancer support yet. 

In this method, a load balancer gets the requests from external entities 
and directs them to workers in the cluster. Load balancers are particularly used 
in cloud environments such as AWS or GCE. 

A third approach is to use Ingress Controller. It is a type of load balancer. 
Ingress Controller needs to be installed on the cluster separately. 

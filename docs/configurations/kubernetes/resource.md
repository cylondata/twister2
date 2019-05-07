# Kubernetes Resource Configuration



**twister2.docker.image.for.kubernetes**
<table><tr><td>default</td><td>"twister2/twister2-k8s:0.1.1"</td><tr><td>description</td><td>Twister2 Docker image for Kubernetes</td></table>

**twister2.system.package.uri**
<table><tr><td>default</td><td>"${TWISTER2_DIST}/twister2-core-0.2.1.tar.gz"</td><tr><td>description</td><td>the package uri</td></table>

**twister2.class.launcher**
<table><tr><td>default</td><td>edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesLauncher</td><tr><td>description</td><td></td></table>

**twister2.class.uploader**
<table><tr><td>default</td><td>"edu.iu.dsc.tws.rsched.uploaders.scp.ScpUploader"</td><tr><td>options</td><td>"edu.iu.dsc.tws.rsched.uploaders.NullUploader"<br/>"edu.iu.dsc.tws.rsched.uploaders.localfs.LocalFileSystemUploader"</td><tr><td>description</td><td>the uplaoder class</td></table>

**kubernetes.image.pull.policy**
<table><tr><td>default</td><td>"Always"</td><tr><td>description</td><td>image pull policy, by default is IfNotPresent<br/>it could also be Always</td></table>

### Kubernetes Mapping and Binding parameters<br/>Statically bind workers to CPUs<br/>Workers do not move from the CPU they are started during computation<br/>twister2.cpu_per_container has to be an integer<br/>by default, its value is false
**kubernetes.worker.to.node.mapping**
<table><tr><td>default</td><td>true</td><tr><td>description</td><td>kubernetes can map workers to nodes as specified by the user<br/>default value is false</td></table>

**kubernetes.worker.mapping.key**
<table><tr><td>default</td><td>"kubernetes.io/hostname"</td><tr><td>description</td><td>the label key on the nodes that will be used to map workers to nodes</td></table>

**kubernetes.worker.mapping.operator**
<table><tr><td>default</td><td>"In"</td><tr><td>options</td><td>https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#node-affinity-beta-feature</td><tr><td>description</td><td>operator to use when mapping workers to nodes based on key value<br/>Exists/DoesNotExist checks only the existence of the specified key in the node.</td></table>

**kubernetes.worker.mapping.values**
<table><tr><td>default</td><td>['e012', 'e013']</td><tr><td>options</td><td>[]</td><tr><td>description</td><td>values for the mapping key<br/>when the mapping operator is either Exists or DoesNotExist, values list must be empty.</td></table>

**Valid values**
<table><tr><td>default</td><td>all-same-node, all-separate-nodes, none</td><tr><td>options</td><td>"all-same-node"</td><tr><td>description</td><td>uniform worker mapping<br/>default value is none</td></table>

### ZooKeeper related config parameters
**twister2.zookeeper.server.addresses**
<table><tr><td>default</td><td>"ip:port"</td><tr><td>options</td><td>"127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"</td><tr><td>description</td><td></td></table>

**#twister2.zookeeper.root.node.path**
<table><tr><td>default</td><td>"/twister2"</td><tr><td>description</td><td>the root node path of this job on ZooKeeper<br/>the default is "/twister2"</td></table>


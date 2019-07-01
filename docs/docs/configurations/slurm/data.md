# Slurm Data Configuration

**twister2.data.hadoop.home**
<table><tr><td>default</td><td>"${HADOOP_HOME}"</td><tr><td>description</td><td>Home directory of the hadoop</td></table>

**twister2.data.hdfs.url**
<table><tr><td>default</td><td>"hdfs://namenodename:9000/"</td><tr><td>description</td><td>URL of the namenode with port</td></table>

**twister2.data.hdfs.class**
<table><tr><td>default</td><td>"org.apache.hadoop.hdfs.DistributedFileSystem"</td><tr><td>description</td><td>Hdfs class</td></table>

**twister2.data.hdfs.implementation.key**
<table><tr><td>default</td><td>"fs.hdfs.impl"</td><tr><td>description</td><td>Hdfs key</td></table>

**twister2.data.hdfs.config.directory**
<table><tr><td>default</td><td>"${HADOOP_HOME}/etc/hadoop/core-site.xml"</td><tr><td>description</td><td>Config directory of the hdfs</td></table>

**twister2.data.hdfs.data.directory**
<table><tr><td>default</td><td>"/user/username/"</td><tr><td>description</td><td>Data directory of the hdfs</td></table>

**twister2.data.hdfs.namenode**
<table><tr><td>default</td><td>"namenodename"</td><tr><td>description</td><td>Name of the namenode</td></table>

**twister2.data.hdfs.namenode.port**
<table><tr><td>default</td><td>"9000"</td><tr><td>description</td><td>Port Number of the namenode</td></table>



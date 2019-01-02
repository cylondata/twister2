<span style="display:block;text-align:left">![Geoffrey C. FOX](fox.png)</span>


# Data Representation

### Twister2 and HDFS Configuration File

The Hadoop Distributed File System (HDFS) (https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html) is a high fault-tolerant distributed file system.
It provides high throughput access to the data which is suitable for the applications require large datasets. It supports the master/slave architecture
which consists of namenode and datanode.

Twister2 provides the support for both local file system and distributed file system. The main configuration file for integrating Twister2 with HDFS
is data.yaml configuration file. The main contents of data.yaml are given below.

### data.yaml file

The user has to specify their HDFS cluster namenode in twister2.hdfs.url and twister2.hdfs.namenode. Also, they have to
specify the respective data directory name as twister2.hdfs.data.directory.

\`\`yaml

```text
twister2.hadoop.home: "${HADOOP_HOME}"

twister2.hdfs.url: "hdfs://namenodename:9000//"

twister2.hdfs.class: "org.apache.hadoop.hdfs.DistributedFileSystem"

twister2.hdfs.implementation.key: "fs.hdfs.impl"

twister2.hdfs.config.directory: "${HADOOP_HOME}/etc/hadoop/core-site.xml"

twister2.hdfs.data.directory: "/user/data-directoryname/"

twister2.hdfs.namenode: "namenodename"

twister2.hdfs.namenode.port: "9000"
```

\`\`

#### HDFS Data Context Class

The main contents of HDFS Data Context class is

```text
    private static final String HDFS_URL = "twister2.hdfs.url";
    private static final String HDFS_URL_DEFAULT = "hdfs://namenodename:9000//";

    private static final String HADOOP_HOME = "twister2.hadoop.home";
    private static final String HADOOP_HOME_DEFAULT = "${HADOOP_HOME}";

    private static final String HDFS_CLASS = "twister2.hdfs.class";
    private static final String HDFS_CLASS_DEFAULT = "org.apache.hadoop.hdfs.DistributedFileSystem";

    private static final String HDFS_IMPLEMENTATION_KEY = "twister2.hdfs.implementation.key";
    private static final String HDFS_IMPLEMENTATION_KEY_DEFAULT = "fs.hdfs.impl";

    private static final String HDFS_CONFIG_DIRECTORY = "twister2.hdfs.config.directory";
    private static final String HDFS_CONFIG_DIRECTORY_DEFAULT = "$HADOOP_HOME/etc/hadoop/core-site.xml";

    private static final String HDFS_DATA_DIRECTORY = "twister2.hdfs.data.directory";
    private static final String HDFS_DATA_DIRECTORY_DEFAULT = "/user/datadirectoryname/";

    private static final String HDFS_NAMENODE_NAME = "twister2.hdfs.namenode";
    private static final String HDFS_NAMENODE_DEFAULT = "namenodename";

    private static final String HDFS_NAMENODE_PORT = "twister2.hdfs.namenode.port";
    private static final Integer HDFS_NAMENODE_PORT_DEFAULT = 9000;
```



<span style="color: green"> More content will be added soon.... </span>


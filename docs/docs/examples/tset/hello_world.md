---
id: hello_world
title: Hello Twister2
sidebar_label: General
---

## About this example

With twister2, you get the ability to spawn a set of processes across
a cluster. These processes can be configured to collectively execute a set of instructions on your data based on your data analytics requirement.



## Defining a Twister2 Job

<!--DOCUSAURUS_CODE_TABS-->
<!--Java-->
```java
JobConfig jobConfig = new JobConfig();

Twister2Job job = Twister2Job.newBuilder()
        .setJobName("Hello Twister2 Job")
        .setConfig(jobConfig)
        .setWorkerClass(HelloTwister2.class)
        .addComputeResource(1, 512, 4)
        .build();
```

<!--Python-->
```python
env = Twister2Environment(resources=[{"cpu": 1, "ram": 512, "instances": 4}])
```

<!--END_DOCUSAURUS_CODE_TABS-->
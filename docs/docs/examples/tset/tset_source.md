---
id: tset_source
title: TSet Source
sidebar_label: TSet Source
---

## About this example

Twister2 is a composable framework for high performance data analytics. twister2 has the capability to handle terabytes of data and can be configured to minimize the latency and improve throughput based on the requirement.

Every twister2 TSet application can be started with one or more data sources. Data source can be backed by local disk, hdfs or even from a database.

## Defining a TSet Source

TSet sources can be created through the TSetEnvironment.

<!--DOCUSAURUS_CODE_TABS-->
<!--Java-->
```java
SourceTSet<Integer> sourceX = env.createSource(new SourceFunc<Integer>() {

      private int count = 0;

      @Override
      public boolean hasNext() {
        return count < 10;
      }

      @Override
      public Integer next() {
        return count++;
      }
}, 4);

sourceX.direct().forEach(i -> {
      LOG.info("i : " + i);
    });
```

<!--Python-->
```python
class IntSource(SourceFunc):

    def __init__(self):
        super().__init__()
        self.i = 0

    def has_next(self):
        return self.i < 10

    def next(self):
        res = self.i
        self.i = self.i + 1
        return res

source_x = env.create_source(IntSource(), 4)

source_x.for_each(lambda i: print("i : %d" % i))
```
<!--END_DOCUSAURUS_CODE_TABS-->

Twister2 internally calls the hasNext function to check if anymore data is available from the source. If data is available, it will call next() function to retrieve and feed the data into the pipeline.

## Running this example

<!--DOCUSAURUS_CODE_TABS-->
<!--Java-->
```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.tset.tutorial.simple.source.TSetSourceExample
```
<!--Python-->
```bash
./bin/twister2 submit standalone python examples/python/tset_source_example.py
```
<!--END_DOCUSAURUS_CODE_TABS-->

## Output

We should see 4 similar responses from each worker printing the i value from 0 to 9

<!--DOCUSAURUS_CODE_TABS-->
<!--Java-->
```bash
[2019-11-27 10:44:17 -0500] [INFO] [worker-1] [main] edu.iu.dsc.tws.examples.tset.tutorial.simple.source.TSetSourceExample: i : 0  
[2019-11-27 10:44:17 -0500] [INFO] [worker-1] [main] edu.iu.dsc.tws.examples.tset.tutorial.simple.source.TSetSourceExample: i : 1  
[2019-11-27 10:44:17 -0500] [INFO] [worker-1] [main] edu.iu.dsc.tws.examples.tset.tutorial.simple.source.TSetSourceExample: i : 2  
[2019-11-27 10:44:17 -0500] [INFO] [worker-1] [main] edu.iu.dsc.tws.examples.tset.tutorial.simple.source.TSetSourceExample: i : 3  
[2019-11-27 10:44:17 -0500] [INFO] [worker-1] [main] edu.iu.dsc.tws.examples.tset.tutorial.simple.source.TSetSourceExample: i : 4  
[2019-11-27 10:44:17 -0500] [INFO] [worker-1] [main] edu.iu.dsc.tws.examples.tset.tutorial.simple.source.TSetSourceExample: i : 5  
[2019-11-27 10:44:17 -0500] [INFO] [worker-1] [main] edu.iu.dsc.tws.examples.tset.tutorial.simple.source.TSetSourceExample: i : 6  
[2019-11-27 10:44:17 -0500] [INFO] [worker-1] [main] edu.iu.dsc.tws.examples.tset.tutorial.simple.source.TSetSourceExample: i : 7  
[2019-11-27 10:44:17 -0500] [INFO] [worker-1] [main] edu.iu.dsc.tws.examples.tset.tutorial.simple.source.TSetSourceExample: i : 8  
[2019-11-27 10:44:17 -0500] [INFO] [worker-1] [main] edu.iu.dsc.tws.examples.tset.tutorial.simple.source.TSetSourceExample: i : 9  
```
<!--Python-->
```bash
i : 0
i : 1
i : 2
i : 3
i : 4
i : 5
i : 6
i : 7
i : 8
i : 9 
```
<!--END_DOCUSAURUS_CODE_TABS-->
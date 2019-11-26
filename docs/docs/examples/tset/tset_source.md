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
```
<!--END_DOCUSAURUS_CODE_TABS-->

Twister2 internally calls the hasNext function to check if anymore data is available from the source. If data is available, it will call next() function to retrieve and feed the data into the pipeline.


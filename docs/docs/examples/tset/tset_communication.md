---
id: tset_communication
title: TSet Communication
sidebar_label: TSet Communication
---

## About this example

The compute operations discussed in the previous section, performs it's calculations withing the same node consuming the data available within the node.

Twister2 has another set of communication operations which can be used to distribute(partition, broadcast), gather or reduce data across the network. Twister2 internally do various optimizations on these operations to effectively utlize the resources(mainly network, cpu and memory) of the cluster. This example shows how a reduce operation can be chained with local compute operations.

## TSet Reduce Operation

TSet sources can be created through the TSetEnvironment.

<!--DOCUSAURUS_CODE_TABS-->
<!--Java-->
```java
sourceX.direct().compute((itr, collector) -> {
      itr.forEachRemaining(i -> {
        collector.collect(i * 5);
      });
    }).direct().compute((itr, collector) -> {
      itr.forEachRemaining(i -> {
        collector.collect((int) i + 2);
      });
    }).reduce((i1, i2) -> {
      return (int) i1 + (int) i2;
    }).forEach(i -> {
      LOG.info("SUM=" + i);
    });
```

<!--Python-->
For any operation, you could define your logic inside a concrete python function or even in a lambda expression. 
```python
def mul_by_five(itr, collector, ctx: TSetContext):
    for i in itr:
        collector.collect(i * 5)


def add_two(itr, collector, ctx: TSetContext):
    for i in itr:
        collector.collect(i + 2)


source_x.compute(mul_by_five).compute(add_two).reduce(lambda i1, i2: i1 + i2) \
    .for_each(lambda i: print("SUM = %d" % i))
```
<!--END_DOCUSAURUS_CODE_TABS-->


TSet API has many more communication operations similar to reduce(). More information on TSet API can be found in [TSet Docs](https://twister2.org/docs/concepts/tset_api).

## Running this example

<!--DOCUSAURUS_CODE_TABS-->
<!--Java-->
```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.tset.tutorial.intermediate.comm.TSetCommunicationExample
```
<!--Python-->
```bash
./bin/twister2 submit standalone python examples/python/tset_communication.py
```
<!--END_DOCUSAURUS_CODE_TABS-->

## Output

We should see a single response showing the sum of all the previous calculations. This time, the output will be printed only from worker 0, as we have applied a reduce operation.

<!--DOCUSAURUS_CODE_TABS-->
<!--Java-->
```bash
[2019-11-27 11:11:48 -0500] [INFO] [worker-0] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.comm.TSetCommunicationExample: SUM=980 
```
<!--Python-->
```bash
SUM = 980
```
<!--END_DOCUSAURUS_CODE_TABS-->


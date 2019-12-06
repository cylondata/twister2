---
id: tset_compute
title: TSet Compute
sidebar_label: TSet Compute
---

## About this example

Data received from the sources can be transformed or perform computations on them by chaining compute operations. This example shows how, compute() chaining works in twister2. 

## TSet Compute

TSet sources can be created through the TSetEnvironment.

<!--DOCUSAURUS_CODE_TABS-->
<!--Java-->
```java
sourceX.direct().compute((itr, collector) -> {
      // if each element of the iterator should be processed individually, compute
      // function which accepts a ComputeCollector can be used.
      itr.forEachRemaining(i -> {
        collector.collect(i * 5);
      });
    }).direct().compute((itr, collector) -> {
      itr.forEachRemaining(i -> {
        collector.collect((int) i + 2);
      });
    }).direct().forEach(i -> {
      LOG.info("(i x 5 ) + 2 = " + i);
    });
```

<!--Python-->
Compute functions should be self executable. They can't refer to outside variables, functions or imports. Whatever required should be imported within the function itself.
```python
class IntSource(SourceFunc):
def mul_by_five(itr, collector, ctx: TSetContext):
    for i in itr:
        collector.collect(i * 5)


def add_two(itr, collector, ctx: TSetContext):
    for i in itr:
        collector.collect(i + 2)


source.compute(mul_by_five).compute(add_two).for_each(lambda i: print("(x * 5) + 2 = %d" % i))
```
<!--END_DOCUSAURUS_CODE_TABS-->

Twister2 has two variants of compute functions. 

1. Compute with a single return value

In this variant, you receive an iterator of data from previous operation
and you can process them internally within the compute and output a single value.

2. Compute with an output collector

In this variant, you receive an iterator of data from previous operation and you can output multiple values as the output through the collector. 

TSet API has many other utility functions that works similar to compute(). More information on TSet API can be found in [TSet Docs](https://twister2.org/docs/concepts/tset_api).

## Running this example

<!--DOCUSAURUS_CODE_TABS-->
<!--Java-->
```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.tset.tutorial.simple.source.TSetComputeExample
```
<!--Python-->
```bash
./bin/twister2 submit standalone python examples/python/tset_compute.py
```
<!--END_DOCUSAURUS_CODE_TABS-->

## Output

We should see 4 similar responses from each worker printing the (x * 5 ) + 2 values where x varies from 0 to 9.

<!--DOCUSAURUS_CODE_TABS-->
<!--Java-->
```bash
[2019-11-27 10:58:59 -0500] [INFO] [worker-2] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.compute.TSetComputeExample: (x * 5 ) + 2 = 2  
[2019-11-27 10:58:59 -0500] [INFO] [worker-2] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.compute.TSetComputeExample: (x * 5 ) + 2 = 7  
[2019-11-27 10:58:59 -0500] [INFO] [worker-2] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.compute.TSetComputeExample: (x * 5 ) + 2 = 12  
[2019-11-27 10:58:59 -0500] [INFO] [worker-2] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.compute.TSetComputeExample: (x * 5 ) + 2 = 17  
[2019-11-27 10:58:59 -0500] [INFO] [worker-2] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.compute.TSetComputeExample: (x * 5 ) + 2 = 22  
[2019-11-27 10:58:59 -0500] [INFO] [worker-2] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.compute.TSetComputeExample: (x * 5 ) + 2 = 27  
[2019-11-27 10:58:59 -0500] [INFO] [worker-2] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.compute.TSetComputeExample: (x * 5 ) + 2 = 32  
[2019-11-27 10:58:59 -0500] [INFO] [worker-2] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.compute.TSetComputeExample: (x * 5 ) + 2 = 37  
[2019-11-27 10:58:59 -0500] [INFO] [worker-2] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.compute.TSetComputeExample: (x * 5 ) + 2 = 42  
[2019-11-27 10:58:59 -0500] [INFO] [worker-2] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.compute.TSetComputeExample: (x * 5 ) + 2 = 47  

```
<!--Python-->
```bash
(i x 5) + 2 = 2
(i x 5) + 2 = 7
(i x 5) + 2 = 12
(i x 5) + 2 = 17
(i x 5) + 2 = 22
(i x 5) + 2 = 27
(i x 5) + 2 = 32
(i x 5) + 2 = 37
(i x 5) + 2 = 42
(i x 5) + 2 = 47
```
<!--END_DOCUSAURUS_CODE_TABS-->



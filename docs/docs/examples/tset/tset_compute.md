---
id: tset_compute
title: TSet Compute
sidebar_label: TSet Compute
---

## About this example

Data received from the sources can be transformed and perform computations by chaining compute operations. This example shows how, compute() chaining works in twister2. 

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

# TODO : LINK TO TSET NON COMMUNICATION OPS DOC



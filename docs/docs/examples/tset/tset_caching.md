---
id: tset_caching
title: TSet Caching
sidebar_label: TSet Caching
---

## About this example

There are instances where we need to merge two datasets(TSets) to do a single computation. In TSets, although you define your TSet operations in a top down approach, twister2 doesn't execute them straight away unless you call a terminal command. A terminal command can be a sink() or a  forEach() where you actually consume the output or it can even be a cache() or a persist() where you don't consume the output, instead hold the output in memory or disk. 
This example shows how cache() command can be useful when adding a dataset as an input of computation of another dataset.

## TSet Cache Operation

When you call cache() on a TSet, twister2 executes everything in the chain, upto that point and holds the resulting datset in memory. Cached TSets can be added as inputs for subsequent computations. 

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

    ComputeTSet<Object, Iterator<Object>> twoComputes = sourceX.direct().compute((itr, c) -> {
      itr.forEachRemaining(i -> {
        c.collect(i * 5);
      });
    }).direct().compute((itr, c) -> {
      itr.forEachRemaining(i -> {
        c.collect((int) i + 2);
      });
    });

    CachedTSet<Object> cached = twoComputes.cache();
    // when cache is called, twister2 will run everything upto this point and cache the result
    // into the memory. Cached TSets can be added as inputs for other TSets and operations.

    SourceTSet<Integer> sourceZ = env.createSource(new SourceFunc<Integer>() {

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

    ComputeTSet<Integer, Iterator<Integer>> calc = sourceZ.direct().compute(
        new ComputeCollectorFunc<Integer, Iterator<Integer>>() {

          private DataPartitionConsumer<Integer> xValues;

          @Override
          public void prepare(TSetContext context) {
            this.xValues = (DataPartitionConsumer<Integer>) context.getInput("x").getConsumer();
          }

          @Override
          public void compute(Iterator<Integer> zValues, RecordCollector<Integer> output) {
            while (zValues.hasNext()) {
              output.collect(xValues.next() + zValues.next());
            }
          }
        });

    calc.addInput("x", cached);

    calc.direct().forEach(i -> {
      LOG.info("(x * 5) + 2 + z =" + i);
    });
```

<!--Python-->
For any operation, you could define your logic inside a concrete python function or even in a lambda expression. 
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


def mul_by_five(itr, collector, ctx: TSetContext):
    for i in itr:
        collector.collect(i * 5)


def add_two(itr, collector, ctx: TSetContext):
    for i in itr:
        collector.collect(i + 2)


two_computes = source_x.compute(mul_by_five).compute(add_two)

cached = two_computes.cache()

source_z = env.create_source(IntSource(), 4)


def combine_x_and_z(itr, collector, ctx: TSetContext):
    x_values = ctx.get_input("x").consumer()
    for x, z in zip(itr, x_values):
        collector.collect(x + z)


calc = source_z.compute(combine_x_and_z)
calc.add_input("x", cached)

calc.for_each(lambda i: print("(x * 5) + 2 + z = %d" % i))
```
<!--END_DOCUSAURUS_CODE_TABS-->

In this example, when you call cache on two_computes, twister2 performs (x*5)+2 calculation and
holds the resulting values in memory. These values can be then fed into source_z.compute() operation 
through the TSetContext

## Running this example

<!--DOCUSAURUS_CODE_TABS-->
<!--Java-->
```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.tset.tutorial.intermediate.caching.TSetCachingExample
```

<!--Python-->
```bash
./bin/twister2 submit standalone python examples/python/tset_caching.py
```
<!--END_DOCUSAURUS_CODE_TABS-->

## Output

We should see 4 similar responses from each worker printing the (x * 5 ) + 2 + z values where x and z varies from 0 to 9.

<!--DOCUSAURUS_CODE_TABS-->
<!--Java-->
```bash
[2019-11-27 11:20:28 -0500] [INFO] [worker-0] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.caching.TSetCachingExample: (x * 5) + 2 + z =2  
[2019-11-27 11:20:28 -0500] [INFO] [worker-0] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.caching.TSetCachingExample: (x * 5) + 2 + z =8  
[2019-11-27 11:20:28 -0500] [INFO] [worker-0] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.caching.TSetCachingExample: (x * 5) + 2 + z =14  
[2019-11-27 11:20:28 -0500] [INFO] [worker-0] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.caching.TSetCachingExample: (x * 5) + 2 + z =20  
[2019-11-27 11:20:28 -0500] [INFO] [worker-0] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.caching.TSetCachingExample: (x * 5) + 2 + z =26  
[2019-11-27 11:20:28 -0500] [INFO] [worker-0] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.caching.TSetCachingExample: (x * 5) + 2 + z =32  
[2019-11-27 11:20:28 -0500] [INFO] [worker-0] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.caching.TSetCachingExample: (x * 5) + 2 + z =38  
[2019-11-27 11:20:28 -0500] [INFO] [worker-0] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.caching.TSetCachingExample: (x * 5) + 2 + z =44  
[2019-11-27 11:20:28 -0500] [INFO] [worker-0] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.caching.TSetCachingExample: (x * 5) + 2 + z =50  
[2019-11-27 11:20:28 -0500] [INFO] [worker-0] [main] edu.iu.dsc.tws.examples.tset.tutorial.intermediate.caching.TSetCachingExample: (x * 5) + 2 + z =56  
```

<!--Python-->
```bash
(x * 5) + 2 + z = 2
(x * 5) + 2 + z = 8
(x * 5) + 2 + z = 14
(x * 5) + 2 + z = 20
(x * 5) + 2 + z = 26
(x * 5) + 2 + z = 32
(x * 5) + 2 + z = 38
(x * 5) + 2 + z = 44
(x * 5) + 2 + z = 50
(x * 5) + 2 + z = 56
```
<!--END_DOCUSAURUS_CODE_TABS-->



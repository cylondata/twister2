---
id: tset_caching
title: TSet Caching
sidebar_label: TSet Caching
---

## About this example

There are instances where we need to merge two datasets(TSets). In TSets, even you define your TSet operations in a top down approach, twister2 doesn't execute them unless you do a terminal command. A terminal command can be a sink(), forEach() or even a cache(). This example shows how cache() command can be useful when adding a dataset as an input of computation of another dataset.

## TSet Cache Operation

When you call cache() on a TSet, twister2 executes everything in the chain, upto that point and holds the resulting datset in memory.

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



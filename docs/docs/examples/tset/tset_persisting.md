---
id: tset_persisting
title: TSet Persisting
sidebar_label: TSet Persisting
---

## About this example

Everything we discussed so far happens on memory. When our data doesn't fit into the memory, persist() command can be used to offload data directly into the local disk. This makes all previous residuals garbage collectible and helps to free up memory for the upcoming operations. 

## TSet Persist Operation

Other than giving the ability to use disk, persist() command serves another purpose when it comes to fault tolerence. If you run twister2 with fault tolerence enabled, when persist() command is executed, twister2 internally creates a checkpoint and on failure, you can restart the same job from the last persist()ed point.

<!--DOCUSAURUS_CODE_TABS-->
<!--Java-->
```java
SourceTSet<Integer> sourceX = env.createSource(new SourceFunc<Integer>() {

      private int count = 0;

      @Override
      public boolean hasNext() {
        return count < 1000000;
      }

      @Override
      public Integer next() {
        return count++;
      }
    }, 4);

    long t1 = System.currentTimeMillis();
    ComputeTSet<Object, Iterator<Object>> twoComputes = sourceX.direct().compute((itr, c) -> {
      itr.forEachRemaining(i -> {
        c.collect(i * 5);
      });
    }).direct().compute((itr, c) -> {
      itr.forEachRemaining(i -> {
        c.collect((int) i + 2);
      });
    });
    LOG.info("Time for two computes : " + (System.currentTimeMillis() - t1));

    t1 = System.currentTimeMillis();
    PersistedTSet<Object> persist = twoComputes.persist();
    LOG.info("TIme for cache : " + (System.currentTimeMillis() - t1));
    // When persist() is called, twister2 performs all the computations/communication
    // upto this point and persists the result into the disk.
    // This makes previous data garbage collectible and frees some memory.
    // If persist() is called in a checkpointing enabled job, this will create
    // a snapshot at this point and will start straightaway from this point if the
    // job is restarted.

    // Similar to CachedTSets, PersistedTSets can be added as inputs for other TSets and
    // operations


    persist.reduce((i1, i2) -> {
      return (int) i1 + (int) i2;
    }).forEach(i -> {
      LOG.info("SUM=" + i);
    });
```

<!--Python-->
```python
class IntSource(SourceFunc):

    def __init__(self):
        super().__init__()
        self.i = 0

    def has_next(self):
        return self.i < 10000

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


t1 = datetime.now()
two_computes = source_x.compute(mul_by_five).compute(add_two)
t2 = datetime.now()
print("Time taken for two_computes %d" % (t2 - t1).total_seconds())

t1 = datetime.now()
persisted = two_computes.persist()
t2 = datetime.now()
print("Time taken for cache %d" % (t2 - t1).total_seconds())

persisted.reduce(lambda i1, i2: i1 + i2) \
    .for_each(lambda i: print("SUM = %d" % i))
```
<!--END_DOCUSAURUS_CODE_TABS-->





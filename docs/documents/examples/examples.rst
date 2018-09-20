Twister2 Examples
=================

.. toctree::
   :maxdepth: 5
   
   communication
   task_examples
   
We have developed many examples that uses both Twister2 core functions as well as higher level abstractions.   

This guide shows how to run Twister2 Examples

Make sure to install Twister2 before running the examples. The installation insturctioncs can be found in [Installation Guide](../../../INSTALL.md)

## Streaming WordCount Example

In this section we will run a streaming word count example from Twister2. This example only uses communication layer and resource scheduling layer. The threads are managed by the program.

The example code can be found in

```
twister2/examples/src/java/edu/iu/dsc/tws/examples/basic/streaming/wordcount/
```

When we install Twister2, it will compile the examples. Lets go to the installtion directory and run the example.

```
cd bazel-bin/scripts/package/twister2-dist/
./bin/twister2 submit nodesmpi jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.streaming.wordcount.WordCountJob
```
After running the streaming example, your terminal will show the following set of lines :

```
edu.iu.dsc.tws.examples.streaming.wordcount.WordAggregate addValue
INFO: 2 Received words: 2000 map: {=267, oO=52, 8LV=46, gK=47, uZ=52, F=56, H=55, 6y0=48, N=36, whB=53, DIu=52, FX=49, R=50, Ja=45, lC=45, b=49, c=46, d=43, sGJ3=63, h=44, uF=56, oB=41, t=54, 7m4M=40, w=141, 7=48, msSX=52, yR=48, 7UvX=50, 3hHU=49, RN=58, 1N=57, nSA=53, ZR6=55}
```
At this point you must press ``` CTRL + C``` to stop the process. 

This will run 4 executors with 8 tasks. So each executor will have two tasks. The tasks in the first two executors will generate words while, the tasks in the last two executors keep a count on the words. The example uses a key based Gather communication between the source and sink tasks.

## Batch WordCount Example

In this section we will run a batch word count example from Twister2. This example only uses communication layer and resource scheduling layer. The threads are managed by the user program.

The example code can be found in

```
twister2/examples/src/java/edu/iu/dsc/tws/examples/basic/batch/wordcount/
```

When we install Twister2, it will compile the examples. Lets go to the installtion directory and run the example.

```
cd bazel-bin/scripts/package/twister2-dist/
./bin/twister2 submit nodesmpi jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.batch.wordcount.WordCountJob
```

This will run 4 executors with 8 tasks. So each executor will have two tasks. At the first phase, the 0-3 tasks running in each executor will generate words and after they are finished, 5-8 tasks will consume those words and create a count.

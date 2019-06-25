# Streaming Example

## Streaming WordCount Example

In this section we will introduce a streaming word count example from Twister2.
After checking this example, you will have an idea on how to write a streaming example on Twister2.

This example is a streaming word count job. 
It is a continuously running job,  processing an unbounded stream of data

Continuous number of words are generated and the global counts of words are calculated

It uses communication layer and resource scheduling layer

The example code can be found in
* twister2/examples/src/java/edu/iu/dsc/tws/examples/streaming/wordcount/

In order to run the example go to the following directory
* cd bazel-bin/scripts/package/twister2-dist/

And run the command below  using standalone resource scheduler
* ./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.streaming.wordcount.WordCountJob


It will run 4 executors with 8 tasks
* Each executor will have two tasks
* Uses key based gather communication between source and sink tasks
* The tasks in the first two executors will generate words
* The tasks in the last two executors will keep word count.

After running the example, you will see the following output 
* edu.iu.dsc.tws.examples.streaming.wordcount.WordAggregate addValue
INFO: 2 Received words: 2000 map: {=267, oO=52, 8LV=46, gK=47, uZ=52, F=56, H=55, 6y0=48, N=36, whB=53, DIu=52, FX=49, R=50, Ja=45, lC=45, b=49, c=46, d=43, sGJ3=63, h=44, uF=56, oB=41, t=54, 7m4M=40, w=141, 7=48, msSX=52, yR=48, 7UvX=50, 3hHU=49, RN=58, 1N=57, nSA=53, ZR6=55}

At this point you should manually stop the process (CTRL+C) 

You can access to the presentation using the link below
[Streaming Processing Example](https://docs.google.com/presentation/d/17uDBBlQxqzLx3m_inOM9svYvANCEwF2nN1KUYDoqInM/edit#slide=id.p)


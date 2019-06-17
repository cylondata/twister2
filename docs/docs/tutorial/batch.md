# Batch Processing Example


## Batch WordCount Example

In this section we will run a batch word count example from Twister2.
After checking this example, you will have an idea on how to write a batch processing example on Twister2.

Job terminates after processing finite amount of input data
Fixed number of words are generated and the global counts of words are calculated

It uses communication layer and resource scheduling layer

The example code can be found in
* twister2/examples/src/java/edu/iu/dsc/tws/examples/batch/wordcount/

In order to run the example go to the following directory
* cd bazel-bin/scripts/package/twister2-dist/

And run the command below  using standalone resource scheduler
* ./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.batch.wordcount.WordCountJob

It will run 4 executors with 8 tasks
* Each executor will have two tasks
* At the first phase tasks 0-3 running on each executor will generate words
* After the words are generated, task 5-8 will consume those words and count.


You can access to the presentation using the link below

[Batch Processing Example](https://docs.google.com/presentation/d/1hpBcy_-m5AuVJJxPdhX_5hnIVB4vUkiB6My0STp-dLA/edit#slide=id.p)

---
id: started_word_count
title: WordCount  
sidebar_label: WordCount
---

Lets look at a word count example. This is a standard example in every big data system. 

We expect you to follow the [Setup Twister2](setting_up.md) before running these two examples. Both
examples assume you are inside the twister2 binary distribution folder.

## Batch WordCount

In the batch wordcount, a source generates a set of words. A global count for each word is created
at the end.

You can run the example with the following command.

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.batch.wordcount.tset.WordCount 
```

It will produce an output with count for each word. The example uses two workers and 2 sources.

```bash
[INFO] [worker-1] [main] edu.iu.dsc.tws.examples.batch.wordcount.tset.WordCount: KeyValue{key=K, value=3}  
[INFO] [worker-1] [main] edu.iu.dsc.tws.examples.batch.wordcount.tset.WordCount: KeyValue{key=O, value=5}  
[INFO] [worker-1] [main] edu.iu.dsc.tws.examples.batch.wordcount.tset.WordCount: KeyValue{key=WF, value=1}  
[INFO] [worker-1] [main] edu.iu.dsc.tws.examples.batch.wordcount.tset.WordCount: KeyValue{key=oqu, value=1}  
[INFO] [worker-1] [main] edu.iu.dsc.tws.examples.batch.wordcount.tset.WordCount: KeyValue{key=S, value=4}  
[INFO] [worker-1] [main] edu.iu.dsc.tws.examples.batch.wordcount.tset.WordCount: KeyValue{key=W, value=4}
```

Note, that your output will be different as it generates random words.

## Streaming WordCount

In the streaming word count, a source will generate an endless stream of random words to simulate a continuous reading
of a message broker. The wordcount keeps track of the global count of each word it sees through the time.

Following command can be used to run thiis example.

```bash
./bin/twister2 submit standalone jar examples/libexamples-java.jar edu.iu.dsc.tws.examples.streaming.wordcount.tset.WordCount
```

It will produce and endless output with the current count of each word.

```bash
[INFO] [worker-1] [executor-0] edu.iu.dsc.tws.examples.streaming.wordcount.tset.WordCount: 1 Word GkM count 9  
[INFO] [worker-1] [executor-0] edu.iu.dsc.tws.examples.streaming.wordcount.tset.WordCount: 1 Word E count 31  
[INFO] [worker-1] [executor-0] edu.iu.dsc.tws.examples.streaming.wordcount.tset.WordCount: 1 Word 46F count 15  
[INFO] [worker-1] [executor-0] edu.iu.dsc.tws.examples.streaming.wordcount.tset.WordCount: 1 Word AvtF count 7  
[INFO] [worker-1] [executor-0] edu.iu.dsc.tws.examples.streaming.wordcount.tset.WordCount: 1 Word ox5 count 3  
[INFO] [worker-1] [executor-0] edu.iu.dsc.tws.examples.streaming.wordcount.tset.WordCount: 1 Word Y count 9  
[INFO] [worker-1] [executor-0] edu.iu.dsc.tws.examples.streaming.wordcount.tset.WordCount: 1 Word ql count 6
```

Unlike in the batch example, we need to termniate the streaming wordcount by pressing ```CNTR + C```.


---
id: apachebeam
title: Apache Beam Runner
sidebar_label: Apache Beam
---


Apache Beam is an open source, unified model for defining both batch and streaming data-parallel processing pipelines.
Beam provides several open sources SDK's which can he used to build a pipline. This pipline is then 
executed using one of the many distributed processing back-ends supported by Apache beam. Currently 
Frameworks such as Apache Spark and Apache Flink are supported, these are called runners.

Twister2 has a Apache Beam runner that is currently being shipped with the Twister2 release, which we plan to 
include directly into Apache Beam, This runner allows users to run Apache beam piplines with Twister2

Below is an example word count code that runs a Beam pipeline using Twister2 Runner. The actual Beam 
code of the example is found in the "runWordCount" method which builds and runs the Apache Beam
Pipeline. This will be run using Twister2 as the distributed processing backend. 

In order to run this example you can use the following command. That is after you have got the Twister2
distribution setup.

```text
./bin/twister2 submit standalone jar $pathtoJar org.apache.beam.runners.twister2.examples.TestRunner
 -input <path-to-text-input-file> -output <path-to-text-output-file> -workers 1 1>&2 | tee out.txt
```

```java
    package org.apache.beam.runners.twister2.examples;
    
    import java.io.Serializable;
    
    import org.apache.beam.runners.twister2.Twister2LegacyRunner;
    import org.apache.beam.runners.twister2.Twister2PipelineOptions;
    import org.apache.beam.sdk.Pipeline;
    import org.apache.beam.sdk.io.TextIO;
    import org.apache.beam.sdk.metrics.Counter;
    import org.apache.beam.sdk.metrics.Distribution;
    import org.apache.beam.sdk.metrics.Metrics;
    import org.apache.beam.sdk.options.Default;
    import org.apache.beam.sdk.options.Description;
    import org.apache.beam.sdk.options.PipelineOptions;
    import org.apache.beam.sdk.options.PipelineOptionsFactory;
    import org.apache.beam.sdk.options.Validation;
    import org.apache.beam.sdk.transforms.Count;
    import org.apache.beam.sdk.transforms.DoFn;
    import org.apache.beam.sdk.transforms.MapElements;
    import org.apache.beam.sdk.transforms.PTransform;
    import org.apache.beam.sdk.transforms.ParDo;
    import org.apache.beam.sdk.transforms.SimpleFunction;
    import org.apache.beam.sdk.values.KV;
    import org.apache.beam.sdk.values.PCollection;
    
    import edu.iu.dsc.tws.api.config.Config;
    import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
    import edu.iu.dsc.tws.tset.worker.BatchTSetIWorker;
    
    public class WordCount implements Serializable, BatchTSetIWorker {
    
      public static final String TOKENIZER_PATTERN = "[^\\p{L}]+";
    
      @Override
      public void execute(BatchTSetEnvironment env) {
    
        Config config = env.getConfig();
        String input = config.getStringValue("input");
        String output = config.getStringValue("output");
        System.out.println("Rank " + env.getWorkerID());
        Twister2PipelineOptions options = PipelineOptionsFactory.as(Twister2PipelineOptions.class);
        options.setTSetEnvironment(env);
        options.as(Twister2PipelineOptions.class).setRunner(Twister2LegacyRunner.class);
        runWordCount(options, input, output);
      }
    
      /**
       * Concept #2: You can make your pipeline assembly code less verbose by defining your DoFns
       * statically out-of-line. This DoFn tokenizes lines of text into individual words; we pass it to
       * a ParDo in the pipeline.
       */
      static class ExtractWordsFn extends DoFn<String, String> {
        private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
        private final Distribution lineLenDist =
            Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");
    
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
          lineLenDist.update(element.length());
          if (element.trim().isEmpty()) {
            emptyLines.inc();
          }
    
          // Split the line into words.
          String[] words = element.split(TOKENIZER_PATTERN, -1);
    
          // Output each word encountered into the output PCollection.
          for (String word : words) {
            if (!word.isEmpty()) {
              receiver.output(word);
            }
          }
        }
      }
    
      /** A SimpleFunction that converts a Word and Count into a printable string. */
      public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
          return input.getKey() + ": " + input.getValue();
        }
      }
    
      /**
       * A PTransform that converts a PCollection containing lines of text into a PCollection of
       * formatted word counts.
       *
       * <p>Concept #3: This is a custom composite transform that bundles two transforms (ParDo and
       * Count) as a reusable PTransform subclass. Using composite transforms allows for easy reuse,
       * modular testing, and an improved monitoring experience.
       */
      public static class CountWords
          extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> lines) {
    
          // Convert lines of text into individual words.
          PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));
    
          // Count the number of times each word occurs.
          PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());
    
          return wordCounts;
        }
      }
    
      /**
       * Options supported by {@link WordCount}.
       *
       * <p>Concept #4: Defining your own configuration options. Here, you can add your own arguments to
       * be processed by the command-line parser, and specify default values for them. You can then
       * access the options values in your pipeline code.
       *
       * <p>Inherits standard configuration options.
       */
      public interface WordCountOptions extends PipelineOptions {
    
        /**
         * By default, this example reads from a public dataset containing the text of King Lear. Set
         * this option to choose a different input file or glob.
         */
        @Description("Path of the file to read from")
        @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
        String getInputFile();
    
        void setInputFile(String value);
    
        /** Set this required option to specify where to write the output. */
        @Description("Path of the file to write to")
        @Validation.Required
        String getOutput();
    
        void setOutput(String value);
      }
    
      static void runWordCount(Twister2PipelineOptions options, String input, String output) {
        Pipeline p = Pipeline.create(options);
    
        // Concepts #2 and #3: Our pipeline applies the composite CountWords transform, and passes the
        // static FormatAsTextFn() to the ParDo transform.
        p.apply("ReadLines", TextIO.read().from(input))
            .apply(new CountWords())
            .apply(MapElements.via(new FormatAsTextFn()))
            .apply("WriteCounts", TextIO.write().to(output));
    
        p.run().waitUntilFinish();
      }
    }
```

Apache beam provides a rich set of SDK's with many adapters which allow you to integrate with
various systems, you can learn more about Apache Beam by referring the Apache Beam [documentation](https://beam.apache.org/documentation/).
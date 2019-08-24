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

Below is an example code that runs a Beam pipline using Twister2 Runner

```java
    Twister2PipelineOptions options = PipelineOptionsFactory.as(Twister2PipelineOptions.class);
    options.setTSetEnvironment(env);
    options.as(Twister2PipelineOptions.class).setRunner(Twister2LegacyRunner.class);
    String resultPath = "/home/pulasthi/work/twister2/beamtest/testdir";
    Pipeline p = Pipeline.create(options);
    PCollection<String> result =
        p.apply(GenerateSequence.from(0).to(10))
            .apply(
                ParDo.of(
                    new DoFn<Long, String>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) throws Exception {
                        c.output(c.element().toString());
                      }
                    }));

    try {
      result.apply(TextIO.write().to(new URI(resultPath).getPath() + "/part"));
    } catch (URISyntaxException e) {
      LOG.info(e.getMessage());
    }
    p.run();
```

Apache beam provides a rich set of SDK's with many adapters which allow you to integrate with
various systems, you can learn more about Apache Beam by referring the Apache Beam [documentation](https://beam.apache.org/documentation/).
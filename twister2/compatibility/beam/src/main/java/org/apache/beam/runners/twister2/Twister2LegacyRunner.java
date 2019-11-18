/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.twister2;

import java.util.logging.Logger;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;

/**
 * A {@link PipelineRunner} that executes the operations in the pipeline by first translating them
 * to a Twister2 Plan and then executing them either locally or on a Twister2 cluster, depending on
 * the configuration.
 */
public class Twister2LegacyRunner extends PipelineRunner<PipelineResult> {

  private static final Logger LOG = Logger.getLogger(Twister2LegacyRunner.class.getName());
//  private static final String SIDEINPUTS = "sideInputs";
//  private static final String LEAVES = "leaves";
  /**
   * Provided options.
   */
  private final Twister2PipelineOptions options;

  public Twister2LegacyRunner(Twister2PipelineOptions options) {
    this.options = options;
  }

  public static Twister2LegacyRunner fromOptions(PipelineOptions options) {
    Twister2PipelineOptions pipelineOptions =
        PipelineOptionsValidator.validate(Twister2PipelineOptions.class, options);
    return new Twister2LegacyRunner(pipelineOptions);
  }

  @Override
  public PipelineResult run(Pipeline pipeline) {
    // create a worker and pass in the pipeline and then do the translation
    Twister2PiplineExecutionEnvironment env = new Twister2PiplineExecutionEnvironment(options);
    LOG.info("Translating pipeline to Twister2 program.");
    env.translate(pipeline);
    env.execute();
//
//    Config config = ResourceAllocator.loadConfig(new HashMap<>());
//
//    JobConfig jobConfig = new JobConfig();
//    jobConfig.put(SIDEINPUTS, env.getSideInputs());
//    jobConfig.put(LEAVES, env.getLeaves());
//
//    int workers = 1;
//    System.out.println("Start");
//    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
//    jobBuilder.setJobName("beam-test-job");
//    jobBuilder.setWorkerClass(WordCount.class.getName());
//    jobBuilder.addComputeResource(2, 512, 1.0, workers);
//    jobBuilder.setConfig(jobConfig);

    //Currently returning an empty result object since this is not supported yet
    return new Twister2PiplineResult();
  }
}

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

import org.apache.beam.runners.twister2.translators.Twister2BatchPipelineTranslator;
import org.apache.beam.runners.twister2.translators.Twister2PipelineTranslator;
import org.apache.beam.runners.twister2.translators.Twister2StreamPipelineTranslator;
import org.apache.beam.sdk.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** doc. */
public class Twister2PiplineExecutionEnvironment {
  private static final Logger LOG =
      LoggerFactory.getLogger(Twister2PiplineExecutionEnvironment.class);

  private final Twister2PipelineOptions options;
  private Twister2TranslationContext twister2TranslationContext;

  public Twister2PiplineExecutionEnvironment(Twister2PipelineOptions options) {
    this.options = options;
  }

  /**
   * translate the pipline into Twister2 TSet graph.
   *
   * @param pipeline
   */
  public void translate(Pipeline pipeline) {

    // TODO: needs to be set properly based on the job
    final boolean hasUnboundedOutput = false;
    if (hasUnboundedOutput) {
      LOG.info("Found unbounded PCollection. Switching to streaming execution.");
      options.setStreaming(true);
    }

    // Staged files need to be set before initializing the execution environments
    // prepareFilesToStageForRemoteClusterExecution(options);

    Twister2PipelineTranslator translator;
    if (options.isStreaming()) {
      //            this.flinkStreamEnv =
      //                    FlinkExecutionEnvironments.createStreamExecutionEnvironment(
      //                            options, options.getFilesToStage());
      //            if (hasUnboundedOutput &&
      // !flinkStreamEnv.getCheckpointConfig().isCheckpointingEnabled()) {
      //                LOG.warn(
      //                        "UnboundedSources present which rely on checkpointing, but
      // checkpointing is disabled.");
      //            }
      twister2TranslationContext = new Twister2TranslationContext(options);
      translator = new Twister2StreamPipelineTranslator(options, twister2TranslationContext);
    } else {
      twister2TranslationContext = new Twister2BatchTranslationContext(options);
      translator =
          new Twister2BatchPipelineTranslator(
              options, (Twister2BatchTranslationContext) twister2TranslationContext);
    }

    translator.translate(pipeline);
  }

  /** Execute all the task graphs. */
  public void execute() {
    twister2TranslationContext.execute();
  }
}

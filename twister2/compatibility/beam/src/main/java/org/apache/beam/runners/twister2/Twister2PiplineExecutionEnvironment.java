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

import org.apache.beam.runners.twister2.translators.Twister2BatchPipelineTranslator;
import org.apache.beam.runners.twister2.translators.Twister2PipelineTranslator;
import org.apache.beam.runners.twister2.translators.Twister2StreamPipelineTranslator;
import org.apache.beam.sdk.Pipeline;

/**
 * Twister2PiplineExecutionEnvironment.
 */
public class Twister2PiplineExecutionEnvironment {
  private static final Logger LOG = Logger.getLogger(
      Twister2PiplineExecutionEnvironment.class.getName());


  private final Twister2PipelineOptions options;
  private Twister2TranslationContext twister2TranslationContext;
  private Twister2RuntimeContext twister2RuntimeContext;

  public Twister2PiplineExecutionEnvironment(Twister2PipelineOptions options) {
    this.options = options;
    twister2RuntimeContext = new Twister2RuntimeContext();
  }

  /**
   * translate the pipline into Twister2 TSet graph.
   */
  public void translate(Pipeline pipeline) {

    // TODO: needs to be set properly based on the job
    final boolean hasUnboundedOutput = false;
    if (hasUnboundedOutput) {
      LOG.info("Found unbounded PCollection. Switching to streaming execution.");
      options.setStreaming(true);
    }

    Twister2PipelineTranslator translator;
    if (options.isStreaming()) {
      twister2TranslationContext = new Twister2TranslationContext(options, twister2RuntimeContext);
      translator = new Twister2StreamPipelineTranslator(options, twister2TranslationContext);
    } else {
      twister2TranslationContext = new Twister2BatchTranslationContext(options,
          twister2RuntimeContext);
      translator =
          new Twister2BatchPipelineTranslator(
              options, (Twister2BatchTranslationContext) twister2TranslationContext);
    }

    translator.translate(pipeline);
  }

  /**
   * Execute all the task graphs.
   */
  public void execute() {
    twister2TranslationContext.execute();
  }
}

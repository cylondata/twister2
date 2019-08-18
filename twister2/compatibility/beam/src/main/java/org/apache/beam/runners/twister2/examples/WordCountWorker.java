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
package org.apache.beam.runners.twister2.examples;

import edu.iu.dsc.tws.api.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.api.tset.worker.BatchTSetIWorker;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.beam.runners.twister2.Twister2LegacyRunner;
import org.apache.beam.runners.twister2.Twister2PipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** doc. */
public class WordCountWorker implements Serializable, BatchTSetIWorker {
  private static final Logger LOG = LoggerFactory.getLogger(WordCountWorker.class);

  @Override
  public void execute(BatchTSetEnvironment env) {
    System.out.println("Rank " + env.getWorkerID());
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
      LOG.error(e.getMessage());
    }
    p.run();
    System.out.println("Result " + result.toString());
  }
}

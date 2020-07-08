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

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Logger;

import org.apache.beam.runners.twister2.Twister2LegacyRunner;
import org.apache.beam.runners.twister2.Twister2PipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import edu.iu.dsc.tws.api.resource.Twister2Worker;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.tset.env.BatchEnvironment;
import edu.iu.dsc.tws.tset.env.TSetEnvironment;
/**
 * doc.
 */
public class ReadSourceTest implements Serializable, Twister2Worker {
  private static final Logger LOG = Logger.getLogger(ReadSourceTest.class.getName());

  @Override
  public void execute(WorkerEnvironment workerEnv) {
    BatchEnvironment env = TSetEnvironment.initBatch(workerEnv);
    System.out.println("Rank " + env.getWorkerID());
    Twister2PipelineOptions options = PipelineOptionsFactory.as(Twister2PipelineOptions.class);
    options.setTSetEnvironment(env);
    options.as(Twister2PipelineOptions.class).setRunner(Twister2LegacyRunner.class);
    String resultPath = "/tmp/testdir";
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
    System.out.println("Result " + result.toString());
  }
}

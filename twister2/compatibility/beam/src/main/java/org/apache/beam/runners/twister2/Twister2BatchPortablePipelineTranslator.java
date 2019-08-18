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

import java.util.Set;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/**
 * Twister2BatchPortablePipelineTranslator.
 */
public class Twister2BatchPortablePipelineTranslator implements Twister2PortablePipelineTranslator {

  private final ImmutableMap<String, PTransformTranslator> urnToTransformTranslator;

  public Twister2BatchPortablePipelineTranslator() {
    this.urnToTransformTranslator = null;
  }

  interface PTransformTranslator {

    /**
     * Translates transformNode from Beam into the Spark context.
     */
    void translate(
        PipelineNode.PTransformNode transformNode,
        RunnerApi.Pipeline pipeline,
        Twister2BatchTranslationContext context);
  }

  @Override
  public Set<String> knownUrns() {
    // Do not expose Read as a known URN because we only want to support Read
    // through the Java ExpansionService. We can't translate Reads for other
    // languages.
    //    return Sets.difference(
    //        urnToTransformTranslator.keySet(),
    //        ImmutableSet.of(PTransformTranslation.READ_TRANSFORM_URN));
    return null;
  }

  @Override
  public void translate(RunnerApi.Pipeline pipeline) {
  }
}

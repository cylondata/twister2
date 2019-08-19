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
package org.apache.beam.runners.twister2.translators;

import java.util.HashMap;
import java.util.Map;

import org.apache.beam.runners.twister2.Twister2BatchTranslationContext;
import org.apache.beam.runners.twister2.Twister2PipelineOptions;
import org.apache.beam.runners.twister2.translators.batch.AssignWindowTranslatorBatch;
import org.apache.beam.runners.twister2.translators.batch.FlatternTranslatorBatch;
import org.apache.beam.runners.twister2.translators.batch.GroupByKeyTranslatorBatch;
import org.apache.beam.runners.twister2.translators.batch.PCollectionViewTranslatorBatch;
import org.apache.beam.runners.twister2.translators.batch.ParDoMultiOutputTranslatorBatch;
import org.apache.beam.runners.twister2.translators.batch.ReadSourceTranslatorBatch;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * doc.
 */
public class Twister2BatchPipelineTranslator extends Twister2PipelineTranslator {

  private static final Logger LOG = LoggerFactory.getLogger(Twister2BatchPipelineTranslator.class);
  private final Twister2PipelineOptions options;

  /**
   * A map from {@link PTransform} subclass to the corresponding {@link BatchTransformTranslator} to
   * use to translate that transform.
   */
  private static final Map<Class<? extends PTransform>, BatchTransformTranslator>
      TRANSFORM_TRANSLATORS = new HashMap<>();

  private final Twister2BatchTranslationContext translationContext;

  static {
    registerTransformTranslator(Flatten.PCollections.class, new FlatternTranslatorBatch());
    registerTransformTranslator(Read.Bounded.class, new ReadSourceTranslatorBatch());
    registerTransformTranslator(ParDo.MultiOutput.class, new ParDoMultiOutputTranslatorBatch());
    registerTransformTranslator(Window.Assign.class, new AssignWindowTranslatorBatch());
    registerTransformTranslator(GroupByKey.class, new GroupByKeyTranslatorBatch());
    registerTransformTranslator(
        View.CreatePCollectionView.class, new PCollectionViewTranslatorBatch());
  }

  public Twister2BatchPipelineTranslator(
      Twister2PipelineOptions options, Twister2BatchTranslationContext twister2TranslationContext) {
    this.options = options;
    this.translationContext = twister2TranslationContext;
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    LOG.debug("visiting transform {}", node.getTransform());
    PTransform transform = node.getTransform();
    BatchTransformTranslator translator = getTransformTranslator(transform.getClass());
    if (null == translator) {
      throw new IllegalStateException("no translator registered for " + transform);
    }
    translationContext.setCurrentTransform(node.toAppliedPTransform(getPipeline()));
    translator.translateNode(transform, translationContext);
  }

  private BatchTransformTranslator<?> getTransformTranslator(
      Class<? extends PTransform> transformClass) {
    return TRANSFORM_TRANSLATORS.get(transformClass);
  }

  /**
   * Records that instances of the specified PTransform class should be translated by default by the
   * corresponding {@link BatchTransformTranslator}.
   */
  private static <TT extends PTransform> void registerTransformTranslator(
      Class<TT> transformClass,
      BatchTransformTranslator<? extends TT> transformTranslator) {
    if (TRANSFORM_TRANSLATORS.put(transformClass, transformTranslator) != null) {
      throw new IllegalArgumentException("defining multiple translators for " + transformClass);
    }
  }
}

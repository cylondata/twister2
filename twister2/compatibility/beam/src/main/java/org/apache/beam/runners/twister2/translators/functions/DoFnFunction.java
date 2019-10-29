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
package org.apache.beam.runners.twister2.translators.functions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.SideInputHandler;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.twister2.utils.NoOpStepContext;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;

import edu.iu.dsc.tws.api.tset.RecordCollector;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;

/**
 * DoFn function.
 */
public class DoFnFunction<OT, IT>
    implements ComputeCollectorFunc<RawUnionValue, Iterator<WindowedValue<IT>>> {

  private final DoFn<IT, OT> doFn;
  private final transient PipelineOptions pipelineOptions;
  private static final long serialVersionUID = -5701440128544343353L;
  private final Coder<IT> inputCoder;
  private final Map<TupleTag<?>, Coder<?>> outputCoders;
  private final WindowingStrategy<?, ?> windowingStrategy;
  private final Collection<PCollectionView<?>> sideInputs;
  private final TupleTag<OT> mainOutput;
  private transient SideInputHandler sideInputReader;
  private transient DoFnRunner<IT, OT> doFnRunner;
  private final DoFnOutputManager outputManager;
  private final List<TupleTag<?>> sideOutputs;
  private StepContext stepcontext;
  private final DoFnSchemaInformation doFnSchemaInformation;
  private final Map<TupleTag<?>, Integer> outputMap;

  public DoFnFunction(
      PipelineOptions pipelineOptions,
      DoFn<IT, OT> doFn,
      Coder<IT> inputCoder,
      Map<TupleTag<?>, Coder<?>> outputCoders,
      List<TupleTag<?>> sideOutputs,
      WindowingStrategy<?, ?> windowingStrategy,
      Collection<PCollectionView<?>> sideInputs,
      TupleTag<OT> mainOutput,
      DoFnSchemaInformation doFnSchemaInformation,
      Map<TupleTag<?>, Integer> outputMap) {
    this.doFn = doFn;
    this.pipelineOptions = pipelineOptions;
    this.inputCoder = inputCoder;
    this.outputCoders = outputCoders;
    this.windowingStrategy = windowingStrategy;
    this.sideInputs = sideInputs;
    this.mainOutput = mainOutput;
    this.doFnSchemaInformation = doFnSchemaInformation;
    this.sideOutputs = sideOutputs;
    this.stepcontext = new NoOpStepContext();
    this.outputMap = outputMap;
    outputManager = new DoFnOutputManager(this.outputMap);
  }

  @Override
  public void prepare(TSetContext context) {
    sideInputReader = new SideInputHandler(sideInputs, InMemoryStateInternals.<Void>forKey(null));
    outputManager.setup(mainOutput, sideOutputs);

    doFnRunner =
        DoFnRunners.simpleRunner(
            pipelineOptions,
            doFn,
            sideInputReader,
            outputManager,
            mainOutput,
            sideOutputs,
            stepcontext,
            inputCoder,
            outputCoders,
            windowingStrategy,
            doFnSchemaInformation);
  }

  @Override
  public void compute(Iterator<WindowedValue<IT>> input, RecordCollector<RawUnionValue> output) {
    outputManager.clear();

    doFnRunner.startBundle();
    while (input.hasNext()) {
      doFnRunner.processElement(input.next());
    }

    doFnRunner.finishBundle();
    Iterator<RawUnionValue> outputs = outputManager.getOutputs();
    while (outputs.hasNext()) {

      output.collect(outputs.next());
    }
  }

  private static class DoFnOutputManager implements DoFnRunners.OutputManager, Serializable {
    // todo need to figure out how this class types are handled
    private static final long serialVersionUID = 4967375172737408160L;
    private transient List<RawUnionValue> outputs;
    private transient Set<TupleTag<?>> outputTags;
    private final Map<TupleTag<?>, Integer> outputMap;

    DoFnOutputManager(Map<TupleTag<?>, Integer> outputMap) {
      this.outputMap = outputMap;
    }

    @Override
    public <T> void output(TupleTag<T> outputTag, WindowedValue<T> output) {
      if (outputTags.contains(outputTag)) {
        outputs.add(new RawUnionValue(outputMap.get(outputTag), output));
      }
    }

    void setup(TupleTag<?> mainOutput, List<TupleTag<?>> sideOutputs) {
      outputs = new ArrayList<>();
      outputTags = new HashSet<>();
      outputTags.add(mainOutput);
      outputTags.addAll(sideOutputs);
    }

    void clear() {
      outputs.clear();
    }

    Iterator<RawUnionValue> getOutputs() {
      return outputs.iterator();
    }
  }
}

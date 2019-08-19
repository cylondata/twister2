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

import java.util.Collection;
import java.util.Iterator;

import org.apache.beam.runners.twister2.utils.Twister2AssignContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.iu.dsc.tws.api.tset.Collector;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;

/**
 * doc.
 */
public class AssignWindowsFunction<T>
    implements ComputeCollectorFunc<WindowedValue<T>, Iterator<WindowedValue<T>>> {
  private static final Logger LOG = LoggerFactory.getLogger(AssignWindowsFunction.class);

  private final WindowFn<T, BoundedWindow> windowFn;

  public AssignWindowsFunction(WindowFn<T, BoundedWindow> windowFn) {
    this.windowFn = windowFn;
  }

  @Override
  public void compute(Iterator<WindowedValue<T>> input, Collector<WindowedValue<T>> output) {
    WindowedValue<T> element;
    try {
      while (input.hasNext()) {
        element = input.next();
        Collection<BoundedWindow> windows =
            windowFn.assignWindows(new Twister2AssignContext<>(windowFn, element));

        for (BoundedWindow window : windows) {
          output.collect(
              WindowedValue.of(
                  element.getValue(), element.getTimestamp(), window, element.getPane()));
        }
      }
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
  }

  @Override
  public void prepare(TSetContext context) {
  }
}

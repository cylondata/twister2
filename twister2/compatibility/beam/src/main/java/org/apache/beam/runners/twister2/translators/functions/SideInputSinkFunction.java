//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package org.apache.beam.runners.twister2.translators.functions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.beam.runners.twister2.Twister2RuntimeContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;

import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.SinkFunc;

/**
 * Sink Function that collects results.
 */
public class SideInputSinkFunction<T> implements SinkFunc<T> {
  private final Twister2RuntimeContext runtimeContext;

  public SideInputSinkFunction(Twister2RuntimeContext context) {
    this.runtimeContext = context;
  }

  @Override
  public boolean add(T value) {
    //TODO need to complete functionality if needed
    Map<BoundedWindow, List<WindowedValue<KV<?, ?>>>> partitionedElements = new HashMap<>();

    Iterator iterator = (Iterator) value;
    while (iterator.hasNext()) {
      WindowedValue<KV<?, ?>> winValue = (WindowedValue<KV<?, ?>>) iterator.next();
      for (BoundedWindow window : winValue.getWindows()) {
        List<WindowedValue<KV<?, ?>>> windowedValues =
            partitionedElements.computeIfAbsent(window, k -> new ArrayList<>());
        windowedValues.add(winValue);      }
    }
    return true;
  }

  @Override
  public void close() {
  }

  @Override
  public DataPartition<?> get() {
    return null;
  }

  @Override
  public void prepare(TSetContext context) {
  }
}

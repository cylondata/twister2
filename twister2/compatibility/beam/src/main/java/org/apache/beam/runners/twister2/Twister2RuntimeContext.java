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
package org.apache.beam.runners.twister2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.beam.runners.core.InMemoryMultimapSideInputView;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Keeps runtime information which includes results from sideinputs so they
 * can be accessed when needed
 */
public class Twister2RuntimeContext implements Serializable {

  private Map<String, WindowedValue<KV<?, ?>>> sideInputs;
  private static final Materializations.MultimapView EMPTY_MULTMAP_VIEW
      = o -> Collections.EMPTY_LIST;

  public Twister2RuntimeContext() {
    sideInputs = new HashMap<>();
  }

  public <VT> void addSideInput(String key, WindowedValue<KV<?, ?>> winValue) {
    if (!sideInputs.containsKey(key)) {
      sideInputs.put(key, winValue);
    }
  }

  public <T> T getSideInput(PCollectionView<T> view, BoundedWindow window) {
    Map<BoundedWindow, List<WindowedValue<KV<?, ?>>>> partitionedElements = new HashMap<>();
    WindowedValue<KV<?, ?>> winValue = sideInputs.get(view.getTagInternal().getId());
    for (BoundedWindow tbw : winValue.getWindows()) {
      List<WindowedValue<KV<?, ?>>> windowedValues =
          partitionedElements.computeIfAbsent(tbw, k -> new ArrayList<>());
      windowedValues.add(winValue);
    }

    Map<BoundedWindow, T> resultMap = new HashMap<>();

    for (Map.Entry<BoundedWindow, List<WindowedValue<KV<?, ?>>>> elements
        : partitionedElements.entrySet()) {

      ViewFn<Materializations.MultimapView, T> viewFn
          = (ViewFn<Materializations.MultimapView, T>) view.getViewFn();
      Coder keyCoder = ((KvCoder<?, ?>) view.getCoderInternal()).getKeyCoder();
      resultMap.put(
          elements.getKey(),
          (T)
              viewFn.apply(
                  InMemoryMultimapSideInputView.fromIterable(
                      keyCoder,
                      (Iterable)
                          elements.getValue().stream()
                              .map(WindowedValue::getValue)
                              .collect(Collectors.toList()))));
    }
    T result = resultMap.get(window);
    if (result == null) {
      ViewFn<Materializations.MultimapView, T> viewFn
          = (ViewFn<Materializations.MultimapView, T>) view.getViewFn();
      result = viewFn.apply(EMPTY_MULTMAP_VIEW);
    }
    return result;
  }
}

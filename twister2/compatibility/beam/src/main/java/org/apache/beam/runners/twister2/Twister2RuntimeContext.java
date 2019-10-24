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
import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Keeps runtime information which includes results from sideinputs so they
 * can be accessed when needed
 */
public class Twister2RuntimeContext implements Serializable {

  private Map<BoundedWindow, PCollectionView<?>> sideInputs;

  public Twister2RuntimeContext() {
    sideInputs = new HashMap<>();
  }

  public <T> T getSideInput(BoundedWindow window) {
    if (sideInputs.containsKey(window)) {
      return (T) sideInputs.get(window);
    }
    return null;
  }

  public void addSideInput(BoundedWindow window, PCollectionView<?> sideInput) {
    if (!sideInputs.containsKey(window)) {
      sideInputs.put(window, sideInput);
    }
  }
}

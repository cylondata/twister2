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
package edu.iu.dsc.tws.dl.module.mkldnn;

import edu.iu.dsc.tws.dl.data.Activity;
import edu.iu.dsc.tws.dl.module.DynamicContainer;
import edu.iu.dsc.tws.dl.utils.pair.MemoryDataArrayPair;

/**
 * Helper utilities when integrating containers with MKL-DNN
 */
public class MklDnnContainer extends DynamicContainer<Activity> implements MklDnnModule {

  @Override
  public Activity updateOutput(Activity input) {
    return null;
  }

  @Override
  public Activity updateGradInput(Activity input, Activity gradOutput) {
    return null;
  }

  @Override
  public MemoryDataArrayPair initFwdPrimitives(MemoryData[] inputs, Phase phase) {
    return null;
  }

  @Override
  public MemoryDataArrayPair initBwdPrimitives(MemoryData[] grad, Phase phase) {
    return null;
  }

  @Override
  public MemoryData[] inputFormats() {
    return new MemoryData[0];
  }

  @Override
  public MemoryData[] gradInputFormats() {
    return new MemoryData[0];
  }

  @Override
  public MemoryData[] outputFormats() {
    return new MemoryData[0];
  }

  @Override
  public MemoryData[] gradOutputFormats() {
    return new MemoryData[0];
  }

  @Override
  public MemoryData[] gradOutputWeightFormats() {
    return new MemoryData[0];
  }
}

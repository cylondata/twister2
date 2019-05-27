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
package edu.iu.dsc.tws.task.api.window.config;

import edu.iu.dsc.tws.task.api.window.api.BaseWindow;
import edu.iu.dsc.tws.task.api.window.strategy.IWindowStrategy;
import edu.iu.dsc.tws.task.api.window.strategy.count.SlidingCountWindowStratergy;

public class SlidingCountWindow extends BaseWindow {

  public SlidingCountWindow(long windowLength, long slideLength) {
    super(windowLength, slideLength);
  }

  @Override
  public <T> IWindowStrategy<T> getWindowStrategy() {
    return new SlidingCountWindowStratergy<>(this);
  }

  public static SlidingCountWindow of(long windowCount, long slidingCount) {
    return new SlidingCountWindow(windowCount, slidingCount);
  }
}

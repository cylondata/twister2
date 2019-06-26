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
package edu.iu.dsc.tws.task.window.strategy.count;

import edu.iu.dsc.tws.task.window.api.IEvictionPolicy;
import edu.iu.dsc.tws.task.window.api.IWindow;
import edu.iu.dsc.tws.task.window.manage.IManager;
import edu.iu.dsc.tws.task.window.policy.eviction.count.CountEvictionPolicy;
import edu.iu.dsc.tws.task.window.policy.trigger.IWindowingPolicy;
import edu.iu.dsc.tws.task.window.policy.trigger.count.CountWindowPolicy;
import edu.iu.dsc.tws.task.window.strategy.BaseWindowStrategy;

public class SlidingCountWindowStratergy<T> extends BaseWindowStrategy<T> {

  public SlidingCountWindowStratergy(IWindow window) {
    super(window);
  }

  /**
   * Get Window Policy
   * @param windowingManager
   * @param evictionPolicy
   * @return
   */
  @Override
  public IWindowingPolicy<T> getWindowingPolicy(IManager<T> windowingManager,
                                                IEvictionPolicy<T> evictionPolicy) {
    return new CountWindowPolicy<>(window.getSlidingLength(), windowingManager, evictionPolicy);
  }

  @Override
  public IEvictionPolicy<T> getEvictionPolicy() {
    return new CountEvictionPolicy<>(window.getWindowLength());
  }
}

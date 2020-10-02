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

package edu.iu.dsc.tws.task.window.policy.trigger;

import edu.iu.dsc.tws.task.window.config.WindowConfig;
import edu.iu.dsc.tws.task.window.constant.WindowType;

public class WindowingGlobalPolicy extends WindowingPolicy {
  public WindowingGlobalPolicy(WindowType windowType, WindowConfig.Count count) {
    super(windowType, count);
  }

  public WindowingGlobalPolicy(WindowType windowType, WindowConfig.Duration duration) {
    super(windowType, duration);
  }
}

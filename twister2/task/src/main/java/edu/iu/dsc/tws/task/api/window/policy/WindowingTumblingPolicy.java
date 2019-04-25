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
package edu.iu.dsc.tws.task.api.window.policy;

import edu.iu.dsc.tws.task.api.window.config.WindowConfig;
import edu.iu.dsc.tws.task.api.window.constant.WindowType;

public class WindowingTumblingPolicy extends WindowingPolicy {

  public WindowingTumblingPolicy(WindowType windowType, WindowConfig.Count count) {
    super(windowType, count);
  }

  public WindowingTumblingPolicy(WindowType windowType, WindowConfig.Duration duration) {
    super(windowType, duration);
  }
}

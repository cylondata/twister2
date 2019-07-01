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

import edu.iu.dsc.tws.task.window.api.Event;
import edu.iu.dsc.tws.task.window.config.WindowConfig;
import edu.iu.dsc.tws.task.window.constant.WindowType;

public class WindowingPolicy<T> implements IWindowingPolicy<T> {

  private static final long serialVersionUID = -2786001848413534229L;

  private static final String RULE = "WindowType NOT NULL && (Window Count > 0 "
      + "|| Window Duration NOT NULL)";

  private WindowType windowType;

  private WindowConfig.Count count;

  private WindowConfig.Duration duration;

  public WindowingPolicy(WindowType windowType, WindowConfig.Count count) {
    this.windowType = windowType;
    this.count = count;
  }

  public WindowingPolicy(WindowType windowType, WindowConfig.Duration duration) {
    this.windowType = windowType;
    this.duration = duration;
  }

  public WindowType getWindowType() {
    return windowType;
  }

  public WindowConfig.Count getCount() {
    return count;
  }

  public WindowConfig.Duration getDuration() {
    return duration;
  }

  public boolean validate() {
    return this.windowType != null && (this.count.value > 0) || this.duration != null;
  }

  @Override
  public String whyInvalid() {
    return String.format("Rule : %s, Current Config :WindowType : %s, Window Count : %d, "
        + "Window Duration : %s", RULE, this.windowType, this.count.value, this.duration);
  }

  @Override
  public void track(Event<T> event) {

  }

  @Override
  public void reset() {

  }

  @Override
  public void start() {

  }

  @Override
  public void shutdown() {

  }


}

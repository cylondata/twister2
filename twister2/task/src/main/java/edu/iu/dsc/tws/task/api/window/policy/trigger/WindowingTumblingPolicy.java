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
package edu.iu.dsc.tws.task.api.window.policy.trigger;

import edu.iu.dsc.tws.task.api.window.config.WindowConfig;
import edu.iu.dsc.tws.task.api.window.constant.WindowType;

public class WindowingTumblingPolicy extends WindowingPolicy {

  private static final String RULE = "WindowType TUMBLING && (Window Count > 0 "
      + "|| Window Duration NOT NULL)";

  private WindowType windowType;

  private WindowConfig.Count count;

  private WindowConfig.Duration duration;

  public WindowingTumblingPolicy(WindowConfig.Count cnt) {
    super(WindowType.TUMBLING, cnt);
    windowType = WindowType.TUMBLING;
    this.count = cnt;
  }

  public WindowingTumblingPolicy(WindowConfig.Duration dtn) {
    super(WindowType.TUMBLING, dtn);
    this.windowType = WindowType.TUMBLING;
    this.duration = dtn;
  }

  public WindowType getWindowType() {
    return windowType;
  }

  public void setWindowType(WindowType winType) {
    this.windowType = winType;
  }

  public WindowConfig.Count getCount() {
    return count;
  }

  public void setCount(WindowConfig.Count cnt) {
    this.count = cnt;
  }

  public WindowConfig.Duration getDuration() {
    return duration;
  }

  public void setDuration(WindowConfig.Duration dtn) {
    this.duration = dtn;
  }

  @Override
  public boolean validate() {
    return windowType == WindowType.TUMBLING && (count.value > 0 || this.duration != null);
  }

  @Override
  public String whyInvalid() {
    return String.format("Rule : %s, Current Config :WindowType : %s, Window Count : %d, "
        + "Window Duration : %s", RULE, this.windowType, this.count.value, this.duration);
  }
}

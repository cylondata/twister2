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

  private WindowType windowType;

  private WindowConfig.Count count;

  private WindowConfig.Duration duration;

  public WindowingTumblingPolicy(WindowType winType, WindowConfig.Count cnt) {
    super(winType, cnt);
    this.windowType = winType;
    this.count = cnt;
  }

  public WindowingTumblingPolicy(WindowType winType, WindowConfig.Duration dtn) {
    super(winType, dtn);
    this.windowType = winType;
    this.duration = dtn;
  }

  @Override
  public WindowType getWindowType() {
    return windowType;
  }

  public void setWindowType(WindowType winType) {
    this.windowType = winType;
  }

  @Override
  public WindowConfig.Count getCount() {
    return count;
  }

  public void setCount(WindowConfig.Count cnt) {
    this.count = cnt;
  }

  @Override
  public WindowConfig.Duration getDuration() {
    return duration;
  }

  public void setDuration(WindowConfig.Duration dtn) {
    this.duration = dtn;
  }
}

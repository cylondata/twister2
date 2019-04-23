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

import java.io.Serializable;

import edu.iu.dsc.tws.task.api.window.config.WindowConfig;
import edu.iu.dsc.tws.task.api.window.constant.Window;

public class WindowingPolicy implements Serializable {

  private static final long serialVersionUID = -2786001848413534229L;

  private Window window;

  private WindowConfig.Count count;

  private WindowConfig.Duration duration;

  public WindowingPolicy(Window window, WindowConfig.Count count) {
    this.window = window;
    this.count = count;
  }

  public WindowingPolicy(Window window, WindowConfig.Duration duration) {
    this.window = window;
    this.duration = duration;
  }

  public Window getWindow() {
    return window;
  }

  public WindowConfig.Count getCount() {
    return count;
  }

  public WindowConfig.Duration getDuration() {
    return duration;
  }
}

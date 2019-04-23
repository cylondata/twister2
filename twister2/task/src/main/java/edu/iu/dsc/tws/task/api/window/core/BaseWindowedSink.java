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
package edu.iu.dsc.tws.task.api.window.core;

import java.util.logging.Logger;

import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.window.IWindowCompute;
import edu.iu.dsc.tws.task.api.window.api.BaseWindowSink;
import edu.iu.dsc.tws.task.api.window.api.IWindowMessage;
import edu.iu.dsc.tws.task.api.window.config.WindowConfig;
import edu.iu.dsc.tws.task.api.window.constant.Window;
import edu.iu.dsc.tws.task.api.window.manage.WindowManager;
import edu.iu.dsc.tws.task.api.window.policy.WindowingPolicy;

public abstract class BaseWindowedSink<T> extends AbstractSingleWindowDataSink<T>
    implements IWindowCompute<T> {

  private static final Logger LOG = Logger.getLogger(BaseWindowedSink.class.getName());

  public abstract IWindowMessage<T> execute(IWindowMessage<T> windowMessage);

  private WindowManager<T> windowManager;

  private WindowingPolicy windowingPolicy;

  public BaseWindowedSink(WindowingPolicy win) {
    this.windowingPolicy = win;
    this.windowManager = new WindowManager<>(win);
  }

  @Override
  public boolean execute(IMessage<T> message) {
    this.windowManager.execute(message.getContent());
    if (this.windowManager.isDone()) {
      execute(this.windowManager.getWindowMessage());
      this.windowManager.clearWindow();
    }
    return false;
  }

  private BaseWindowSink<T> withWindowCount(Window window, WindowConfig.Count count) {
    WindowingPolicy win = new WindowingPolicy(window, count);
    this.windowManager.addWindowingPolicy(win);
    return this;
  }

  private BaseWindowSink<T> withWindowDuration(Window window, WindowConfig.Duration duration) {
    WindowingPolicy win = new WindowingPolicy(window, duration);
    this.windowManager.addWindowingPolicy(win);
    return this;
  }

}

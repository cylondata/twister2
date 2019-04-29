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

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.window.IWindowCompute;
import edu.iu.dsc.tws.task.api.window.api.IWindowMessage;
import edu.iu.dsc.tws.task.api.window.config.WindowConfig;
import edu.iu.dsc.tws.task.api.window.constant.WindowType;
import edu.iu.dsc.tws.task.api.window.exceptions.InValidWindowingPolicy;
import edu.iu.dsc.tws.task.api.window.manage.WindowManager;
import edu.iu.dsc.tws.task.api.window.policy.IWindowingPolicy;
import edu.iu.dsc.tws.task.api.window.policy.WindowingPolicy;

public abstract class BaseWindowedSink<T> extends AbstractSingleWindowDataSink<T>
    implements IWindowCompute<T> {

  private static final Logger LOG = Logger.getLogger(BaseWindowedSink.class.getName());

  public abstract IWindowMessage<T> execute(IWindowMessage<T> windowMessage);

  private WindowManager<T> windowManager;

  private IWindowingPolicy windowingPolicy;

  private WindowConfig.Count count;

  private WindowType windowType;

  private WindowConfig.Duration duration;

  protected BaseWindowedSink() {
    this.windowManager = new WindowManager<>();
  }

  public BaseWindowedSink(IWindowingPolicy win) throws InValidWindowingPolicy {
    this.windowingPolicy = win;
    this.windowManager = new WindowManager<>(win);
  }

  @Override
  public boolean execute(IMessage<T> message) {
    this.windowManager.execute(message);
    if (this.windowManager.isDone()) {
      execute(this.windowManager.getWindowMessage());
      this.windowManager.clearWindow();
    }
    return false;
  }

  private BaseWindowedSink<T> withWindowCountInit(WindowType winType, WindowConfig.Count cnt) {
    WindowingPolicy win = new WindowingPolicy(winType, cnt);
    this.windowManager.addWindowingPolicy(win);
    return this;
  }

  private BaseWindowedSink<T> withWindowDurationInit(WindowType winType,
                                                     WindowConfig.Duration dtn) {
    WindowingPolicy win = new WindowingPolicy(winType, dtn);
    this.windowManager.addWindowingPolicy(win);
    return this;
  }

  public BaseWindowedSink<T> withWindowCount(WindowType winType, WindowConfig.Count cnt) {
    return withWindowCountInit(winType, cnt);
  }

  public BaseWindowedSink<T> withWindowDuration(WindowType winType,
                                                WindowConfig.Duration dtn) {
    return withWindowDurationInit(winType, dtn);
  }

  public BaseWindowedSink<T> withWindowingPolicy(IWindowingPolicy iWindowingPolicy) {
    this.windowManager.addWindowingPolicy(iWindowingPolicy);
    return this;
  }

  public BaseWindowedSink<T> withTumblingCountWindow(int tumblingCount) {
    this.count = new WindowConfig.Count(tumblingCount);
    this.windowType = WindowType.TUMBLING;
    return withTumblingCountWindowInit(this.count, this.windowType);
  }

  private BaseWindowedSink<T> withTumblingCountWindowInit(WindowConfig.Count cnt,
                                                          WindowType winType) {

    return this;
  }

  public BaseWindowedSink<T> withTumblingDurationWindow(int tumblingDuration, TimeUnit timeUnit) {
    this.duration = new WindowConfig.Duration(tumblingDuration, timeUnit);
    this.windowType = WindowType.TUMBLING;
    return this;
  }


}

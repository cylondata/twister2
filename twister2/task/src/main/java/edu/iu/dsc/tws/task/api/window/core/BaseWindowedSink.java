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

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.api.Closable;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.api.window.IWindowCompute;
import edu.iu.dsc.tws.task.api.window.api.IEvictionPolicy;
import edu.iu.dsc.tws.task.api.window.api.IWindow;
import edu.iu.dsc.tws.task.api.window.api.IWindowMessage;
import edu.iu.dsc.tws.task.api.window.api.WindowLifeCycleListener;
import edu.iu.dsc.tws.task.api.window.config.WindowConfig;
import edu.iu.dsc.tws.task.api.window.exceptions.InvalidWindow;
import edu.iu.dsc.tws.task.api.window.manage.WindowManager;
import edu.iu.dsc.tws.task.api.window.policy.eviction.count.CountEvictionPolicy;
import edu.iu.dsc.tws.task.api.window.policy.eviction.duration.DurationEvictionPolicy;
import edu.iu.dsc.tws.task.api.window.policy.trigger.IWindowingPolicy;
import edu.iu.dsc.tws.task.api.window.policy.trigger.count.CountWindowPolicy;
import edu.iu.dsc.tws.task.api.window.policy.trigger.duration.DurationWindowPolicy;
import edu.iu.dsc.tws.task.api.window.strategy.IWindowStrategy;
import edu.iu.dsc.tws.task.api.window.util.WindowParameter;
import edu.iu.dsc.tws.task.api.window.util.WindowUtils;

public abstract class BaseWindowedSink<T> extends AbstractSingleWindowDataSink<T>
    implements IWindowCompute<T>, Closable {

  private static final Logger LOG = Logger.getLogger(BaseWindowedSink.class.getName());

  public abstract IWindowMessage<T> execute(IWindowMessage<T> windowMessage);

  private WindowManager<T> windowManager;

  private IWindowingPolicy<T> windowingPolicy;

  private WindowParameter windowParameter;

  private WindowLifeCycleListener<T> windowLifeCycleListener;

  private IEvictionPolicy<T> evictionPolicy;

  private IWindow iWindow;

  protected BaseWindowedSink() {
  }

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    this.windowLifeCycleListener = newWindowLifeCycleListener();
    this.windowManager = new WindowManager(this.windowLifeCycleListener);
    initialize();
  }

  public void initialize() {
    try {
      if (this.iWindow == null) {
        this.iWindow = WindowUtils.getWindow(this.windowParameter.getWindowCountSize(),
            this.windowParameter.getSlidingCountSize(),
            this.windowParameter.getWindowDurationSize(),
            this.windowParameter.getSldingDurationSize());
      }
      IWindowStrategy<T> windowStrategy = this.iWindow.getWindowStrategy();
      this.evictionPolicy = windowStrategy.getEvictionPolicy();
      this.windowingPolicy = windowStrategy.getWindowingPolicy(this.windowManager,
          this.evictionPolicy);
      this.windowManager.setEvictionPolicy(this.evictionPolicy);
      this.windowManager.setWindowingPolicy(this.windowingPolicy);
      start();
    } catch (InvalidWindow invalidWindow) {
      invalidWindow.printStackTrace();
    }
  }

  @Override
  public boolean execute(IMessage<T> message) {
    this.windowManager.add(message);
    return true;
  }

  public BaseWindowedSink<T> withTumblingCountWindow(long tumblingCount) {
    this.windowParameter = new WindowParameter();
    this.windowParameter.withTumblingCountWindow(tumblingCount);
    return this;
  }

  public BaseWindowedSink<T> withTumblingDurationWindow(long tumblingDuration, TimeUnit timeUnit) {
    this.windowParameter = new WindowParameter();
    this.windowParameter.withTumblingDurationWindow(tumblingDuration, timeUnit);
    return this;
  }

  public BaseWindowedSink<T> withSlidingCountWindow(long windowCount, long slidingCount) {
    this.windowParameter = new WindowParameter();
    this.windowParameter.withSlidingingCountWindow(windowCount, slidingCount);
    return this;
  }

  public BaseWindowedSink<T> withSlidingDurationWindow(long windowDuration, TimeUnit windowTU,
                                                       long slidingDuration, TimeUnit slidingTU) {
    this.windowParameter = new WindowParameter();
    this.windowParameter.withSlidingDurationWindow(windowDuration, windowTU, slidingDuration,
        slidingTU);
    return this;
  }

  public BaseWindowedSink<T> withWindow(IWindow window) {
    this.iWindow = window;
    return this;
  }


  protected WindowLifeCycleListener<T> newWindowLifeCycleListener() {
    return new WindowLifeCycleListener<T>() {
      @Override
      public void onExpiry(IWindowMessage<T> events) {
        // TODO : design the logic
      }

      @Override
      public void onActivation(IWindowMessage<T> events, IWindowMessage<T> newEvents,
                               IWindowMessage<T> expired) {
        execute(events);
      }
    };
  }

  public IWindowingPolicy<T> getWindowingPolicy(WindowConfig.Count slidingIntervalCount,
                                                WindowConfig.Duration slidingIntervalDuration,
                                                WindowManager<T> manager,
                                                IEvictionPolicy<T> policy) {
    if (slidingIntervalCount != null) {
      return new CountWindowPolicy<>(slidingIntervalCount.value, manager, policy);
    } else {
      return new DurationWindowPolicy<>(slidingIntervalDuration.value, manager, policy);
    }
  }

  public IEvictionPolicy<T> getEvictionPolicy(WindowConfig.Count windowLengthCount,
                                              WindowConfig.Duration windowLengthDuration) {
    if (windowLengthCount != null) {
      return new CountEvictionPolicy<>(windowLengthCount.value);
    } else {
      return new DurationEvictionPolicy<>(windowLengthDuration.value);
    }
  }

  public void start() {
    this.windowingPolicy.start();
  }

  @Override
  public void close() {
    this.windowManager.shutdown();
  }

  @Override
  public void refresh() {

  }
}

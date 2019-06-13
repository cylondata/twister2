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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.api.Closable;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.api.window.IWindowCompute;
import edu.iu.dsc.tws.task.api.window.api.GlobalStreamId;
import edu.iu.dsc.tws.task.api.window.api.IEvictionPolicy;
import edu.iu.dsc.tws.task.api.window.api.ITimestampExtractor;
import edu.iu.dsc.tws.task.api.window.api.IWindow;
import edu.iu.dsc.tws.task.api.window.api.IWindowMessage;
import edu.iu.dsc.tws.task.api.window.api.WindowLifeCycleListener;
import edu.iu.dsc.tws.task.api.window.config.WindowConfig;
import edu.iu.dsc.tws.task.api.window.event.WatermarkEventGenerator;
import edu.iu.dsc.tws.task.api.window.exceptions.InvalidWindow;
import edu.iu.dsc.tws.task.api.window.manage.WindowManager;
import edu.iu.dsc.tws.task.api.window.policy.eviction.count.CountEvictionPolicy;
import edu.iu.dsc.tws.task.api.window.policy.eviction.count.WatermarkCountEvictionPolicy;
import edu.iu.dsc.tws.task.api.window.policy.eviction.duration.DurationEvictionPolicy;
import edu.iu.dsc.tws.task.api.window.policy.eviction.duration.WatermarkDurationEvictionPolicy;
import edu.iu.dsc.tws.task.api.window.policy.trigger.IWindowingPolicy;
import edu.iu.dsc.tws.task.api.window.policy.trigger.count.CountWindowPolicy;
import edu.iu.dsc.tws.task.api.window.policy.trigger.count.WatermarkCountWindowPolicy;
import edu.iu.dsc.tws.task.api.window.policy.trigger.duration.DurationWindowPolicy;
import edu.iu.dsc.tws.task.api.window.policy.trigger.duration.WatermarkDurationWindowPolicy;
import edu.iu.dsc.tws.task.api.window.strategy.IWindowStrategy;
import edu.iu.dsc.tws.task.api.window.util.WindowParameter;
import edu.iu.dsc.tws.task.api.window.util.WindowUtils;

public abstract class BaseWindowedSink<T> extends AbstractSingleWindowDataSink<T>
    implements IWindowCompute<T>, Closable {

  private static final Logger LOG = Logger.getLogger(BaseWindowedSink.class.getName());

  public abstract boolean execute(IWindowMessage<T> windowMessage);

  public abstract boolean getExpire(IWindowMessage<T> expiredMessages);

  public abstract boolean getLateMessages(IMessage<T> lateMessages);

  private static final long DEFAULT_WATERMARK_INTERVAL = 1000; // 1s

  private static final long DEFAULT_MAX_LAG = 0; // 0s

  private long maxLagMs = 0;

  private WindowConfig.Duration watermarkInterval = null;

  private WindowConfig.Duration allowedLateness = null;

  private WindowManager<T> windowManager;

  private IWindowingPolicy<T> windowingPolicy;

  private WindowParameter windowParameter;

  private WindowLifeCycleListener<T> windowLifeCycleListener;

  private IEvictionPolicy<T> evictionPolicy;

  private IWindow iWindow;

  private T collectiveOutput;

  private IWindowMessage<T> collectiveEvents;

  private ITimestampExtractor<T> iTimestampExtractor;

  private WatermarkEventGenerator<T> watermarkEventGenerator;


  protected BaseWindowedSink() {
  }

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    super.prepare(cfg, ctx);
    this.windowLifeCycleListener = newWindowLifeCycleListener();
    this.windowManager = new WindowManager(this.windowLifeCycleListener);
    initialize(ctx);
  }

  public void initialize(TaskContext context) {
    try {
      if (this.iWindow == null) {
        this.iWindow = WindowUtils.getWindow(this.windowParameter.getWindowCountSize(),
            this.windowParameter.getSlidingCountSize(),
            this.windowParameter.getWindowDurationSize(),
            this.windowParameter.getSldingDurationSize());
      }

      if (iTimestampExtractor != null) {
        // TODO : handle delayed Stream

        long watermarkInt = 1;
        // TODO : handle this from a config param
        if (this.watermarkInterval != null) {
          watermarkInt = this.watermarkInterval.value;
        } else {
          watermarkInt = DEFAULT_WATERMARK_INTERVAL;
        }

        if (this.allowedLateness != null) {
          maxLagMs = this.allowedLateness.value;
        } else {
          maxLagMs = DEFAULT_MAX_LAG;
        }

        watermarkEventGenerator = new WatermarkEventGenerator(this.windowManager,
            maxLagMs, watermarkInt, getComponentStreams(context));
      }
      setPolicies(this.iWindow.getWindowStrategy());
      start();
    } catch (InvalidWindow invalidWindow) {
      invalidWindow.printStackTrace();
    }
  }

  @Override
  public boolean execute(IMessage<T> message) {
    if (isTimestamped()) {
      long time = iTimestampExtractor.extractTimestamp(message.getContent());
      GlobalStreamId streamId = new GlobalStreamId(message.edge());
      if (watermarkEventGenerator.track(streamId, time)) {
        this.windowManager.add(message, time);
      } else {
        // TODO : handle the late tuple stream a delayed message won't be handled unless a
        //  late stream
        // TODO : Here another latemessage function can be called or we can bundle the late messages
        //  with the next windowing message
        getLateMessages(message);
      }
    } else {
      this.windowManager.add(message);
    }

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

  public BaseWindowedSink<T> withCustomTimestampExtractor(ITimestampExtractor timestampExtractor) {
    this.iTimestampExtractor = timestampExtractor;
    return this;
  }

  public BaseWindowedSink<T> withTimestampExtractor() {
    this.iTimestampExtractor = null;
    return this;
  }

  public BaseWindowedSink<T> withAllowedLateness(long lateness, TimeUnit timeUnit) {
    this.allowedLateness = new WindowConfig.Duration(lateness, timeUnit);
    return this;
  }

  public BaseWindowedSink<T> withWatermarkInterval(long watermarkInt, TimeUnit timeUnit) {
    this.watermarkInterval = new WindowConfig.Duration(watermarkInt, timeUnit);
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
        getExpire(events);
      }

      @Override
      public void onActivation(IWindowMessage<T> events, IWindowMessage<T> newEvents,
                               IWindowMessage<T> expired) {
        collectiveEvents = events;
        execute(collectiveEvents);
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

  public void setPolicies(IWindowStrategy<T> windowStrategy) {
    // Setting Eviction Policies
    IEvictionPolicy<T> eviPolicy = windowStrategy.getEvictionPolicy();
    if (isTimestamped()) {
      if (eviPolicy instanceof CountEvictionPolicy) {
        LOG.info(String.format("WatermarkCountEvictionPolicy selected"));
        this.evictionPolicy = new WatermarkCountEvictionPolicy(this.windowParameter
            .getWindowCountSize().value);
      }
      if (eviPolicy instanceof DurationEvictionPolicy) {
        LOG.info(String.format("WatermarkDurationEvictionPolicy selected"));
        this.evictionPolicy = new WatermarkDurationEvictionPolicy(this.windowParameter
            .getWindowDurationSize().value, maxLagMs);
      }
    } else {
      this.evictionPolicy = eviPolicy;
    }

    // Setting Windowing Policies
    IWindowingPolicy<T> winPolicy = windowStrategy
        .getWindowingPolicy(this.windowManager, this.evictionPolicy);
    if (isTimestamped()) {
      if (winPolicy instanceof CountWindowPolicy) {
        LOG.info(String.format("WatermarkCountWindowingPolicy selected"));
        this.windowingPolicy = new WatermarkCountWindowPolicy(this.windowParameter
            .getSlidingCountSize().value, this.windowManager, this.evictionPolicy,
            this.windowManager);
      }
      if (winPolicy instanceof DurationWindowPolicy) {
        LOG.info(String.format("WatermarkDurationWindowingPolicy selected"));
        this.windowingPolicy = new WatermarkDurationWindowPolicy(this.windowParameter
            .getSldingDurationSize().value, this.windowManager, this.windowManager,
            this.evictionPolicy);
      }
    } else {
      this.windowingPolicy = winPolicy;
    }
    this.windowManager.setEvictionPolicy(this.evictionPolicy);
    this.windowManager.setWindowingPolicy(this.windowingPolicy);
  }


  public void start() {
    if (watermarkEventGenerator != null) {
      LOG.info("Starting WatermarkGenerator");
      LOG.log(Level.FINE, "Starting watermark generator");
      watermarkEventGenerator.start();
    }
    LOG.log(Level.FINE, "Starting windowing policy");
    this.windowingPolicy.start();
  }

  @Override
  public void close() {
    if (watermarkEventGenerator != null) {
      watermarkEventGenerator.shutdown();
    }
    this.windowManager.shutdown();
  }

  @Override
  public void reset() {

  }

  private boolean isTimestamped() {
    return iTimestampExtractor != null;
  }

  private static class WindowedLateOutputCollector<T> {
    private final List<IMessage<T>> messageList;
    private IWindowMessage<T> iWindowMessage = null;

    WindowedLateOutputCollector(List<IMessage<T>> list) {
      messageList = list;
    }


  }

  private Set<GlobalStreamId> getComponentStreams(TaskContext context) {
    Set<GlobalStreamId> streams = new HashSet<>();
    //TODO : Handle the checkpointing edge
    streams = wrapGlobalStreamId(context);
    return streams;
  }

  /**
   * Get the edges connected to this task
   * @param context
   * @return
   */
  private Set<GlobalStreamId> wrapGlobalStreamId(TaskContext context) {
    Set<GlobalStreamId> streams = new HashSet<>();
    for (String s : context.getInputs().keySet()) {
      GlobalStreamId globalStreamId = new GlobalStreamId(s);
      streams.add(globalStreamId);
    }
    return streams;
  }

}

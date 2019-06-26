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
package edu.iu.dsc.tws.task.window.policy.trigger.count;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import edu.iu.dsc.tws.task.window.api.DefaultEvictionContext;
import edu.iu.dsc.tws.task.window.api.Event;
import edu.iu.dsc.tws.task.window.api.IEvictionPolicy;
import edu.iu.dsc.tws.task.window.manage.IManager;
import edu.iu.dsc.tws.task.window.manage.WindowManager;

public class WatermarkCountWindowPolicy<T> extends CountWindowPolicy<T> {

  private final long count;
  private final AtomicInteger currentCount;
  private final IManager manager;
  private final WindowManager<T> windowManager;
  private final IEvictionPolicy<T> evictionPolicy;
  private boolean started;
  private long lastProcessedTimestamp = 0;

  public WatermarkCountWindowPolicy(long count, IManager manager,
                                    IEvictionPolicy<T> evictionPolicy,
                                    WindowManager<T> winManager) {
    super(count, manager, evictionPolicy);
    this.count = count;
    this.manager = manager;
    this.windowManager = winManager;
    this.evictionPolicy = evictionPolicy;
    this.currentCount = new AtomicInteger();
    this.started = false;

  }

  @Override
  public boolean validate() {
    return count > 0;
  }

  @Override
  public String whyInvalid() {
    return null;
  }

  @Override
  public void track(Event<T> event) {
    if (started && event.isWatermark()) {
      onWatermarkEvent(event);
    }
  }

  @Override
  public void reset() {
    // NO IMPLEMENTATION
  }

  @Override
  public void start() {
    started = true;
  }

  @Override
  public void shutdown() {
    // NO IMPLEMENTATION
  }

  private void onWatermarkEvent(Event<T> watermarkEvent) {
    long watermarkTimestamp = watermarkEvent.getTimeStamp();
    List<Long> eventTimestamps = windowManager.getSlidingCountTimestamps(lastProcessedTimestamp,
        watermarkTimestamp, count);
    for (long t : eventTimestamps) {
      evictionPolicy.setContext(new DefaultEvictionContext(t, null, Long.valueOf(count)));
      manager.onEvent();
      lastProcessedTimestamp = t;
    }
  }

  @Override
  public String toString() {
    return "WatermarkCountTriggerPolicy{"
        + "count=" + count
        + ", currentCount=" + currentCount
        + ", manager=" + manager
        + ", windowManager=" + windowManager
        + ", evictionPolicy=" + evictionPolicy
        + ", started=" + started
        + ", lastProcessedTimestamp=" + lastProcessedTimestamp
        + '}';
  }
}

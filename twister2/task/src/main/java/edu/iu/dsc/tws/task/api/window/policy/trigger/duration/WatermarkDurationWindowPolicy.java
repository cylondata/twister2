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
package edu.iu.dsc.tws.task.api.window.policy.trigger.duration;

import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.task.api.window.api.DefaultEvictionContext;
import edu.iu.dsc.tws.task.api.window.api.Event;
import edu.iu.dsc.tws.task.api.window.api.IEvictionPolicy;
import edu.iu.dsc.tws.task.api.window.manage.IManager;
import edu.iu.dsc.tws.task.api.window.manage.WindowManager;

public class WatermarkDurationWindowPolicy<T> extends DurationWindowPolicy<T> {

  private static final Logger LOG = Logger.getLogger(WatermarkDurationWindowPolicy.class.getName());
  private final long slidingInterval;
  private final IManager manager;
  private final WindowManager<T> windowManager;
  private final IEvictionPolicy<T> evictionPolicy;
  private boolean started;
  private long nextWindowEndTime = 0;

  public WatermarkDurationWindowPolicy(long slidingInterval, IManager manager,
                                       WindowManager<T> windowManager,
                                       IEvictionPolicy<T> evictionPolicy) {
    super(slidingInterval, manager, evictionPolicy);
    this.slidingInterval = slidingInterval;
    this.manager = manager;
    this.windowManager = windowManager;
    this.evictionPolicy = evictionPolicy;
    this.started = false;
  }

  @Override
  public boolean validate() {
    return this.slidingInterval > 0;
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

  }

  @Override
  public void start() {
    started = true;
  }

  @Override
  public void shutdown() {

  }

  private void onWatermarkEvent(Event<T> event) {
    long watermarkTimestamp = event.getTimeStamp();
    long windowEndTimestamp = nextWindowEndTime;
    LOG.log(Level.FINE, String.format("Window End Timestamp : %s, Watermark Timestamp : %s",
        String.valueOf(windowEndTimestamp), String.valueOf(watermarkTimestamp)));
    while (windowEndTimestamp <= watermarkTimestamp) {
      long currentEventCount = windowManager.getEventCount(windowEndTimestamp);
      evictionPolicy.setContext(new DefaultEvictionContext(windowEndTimestamp, currentEventCount));
      if (manager.onEvent()) {
        windowEndTimestamp += slidingInterval;
      } else {
        long timestamp = getNextAlignedWindowTimestamp(windowEndTimestamp, watermarkTimestamp);
        LOG.log(Level.FINE, String.format("Next Aligned Window End at timestamp %s",
            String.valueOf(timestamp)));
        if (timestamp == Long.MAX_VALUE) {
          LOG.log(Level.FINE, String.format("No Events to process in time periods of window "
                  + "end timestamp %s and watermark timestamp %s", windowEndTimestamp,
              watermarkTimestamp));
          break;
        }
        windowEndTimestamp = timestamp;
      }
    }
    nextWindowEndTime = windowEndTimestamp;
  }

  private long getNextAlignedWindowTimestamp(long start, long end) {
    long nextTimestamp = windowManager.getEarliestEventTimestamp(start, end);
    if (nextTimestamp == Long.MAX_VALUE || (nextTimestamp % slidingInterval == 0)) {
      return nextTimestamp;
    }
    return nextTimestamp + (slidingInterval - (nextTimestamp % slidingInterval));
  }
}

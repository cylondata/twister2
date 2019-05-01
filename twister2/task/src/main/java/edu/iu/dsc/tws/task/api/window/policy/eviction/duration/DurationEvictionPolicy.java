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
package edu.iu.dsc.tws.task.api.window.policy.eviction.duration;

import java.util.logging.Logger;

import edu.iu.dsc.tws.task.api.window.api.Event;
import edu.iu.dsc.tws.task.api.window.api.IEvictionContext;
import edu.iu.dsc.tws.task.api.window.api.IEvictionPolicy;
import edu.iu.dsc.tws.task.api.window.constant.Action;

public class DurationEvictionPolicy<T> implements IEvictionPolicy<T> {

  private static final Logger LOG = Logger.getLogger(DurationEvictionPolicy.class.getName());

  protected Long referenceTime;

  private long delta;

  private final int windowLength;

  private IEvictionContext evictionContext;


  public DurationEvictionPolicy(int winLength) {
    this.windowLength = winLength;
  }

  @Override
  public Action evict(Event<T> event) {
    long now = referenceTime == null ? System.currentTimeMillis() : referenceTime;
    long diff = now - event.getTimeStamp();
    if (diff >= (windowLength + delta)) {
      return Action.EXPIRE;
    } else if (diff < 0) { // do not process events beyond current ts
      return Action.KEEP;
    }
    return Action.PROCESS;
  }

  @Override
  public void track(Event<T> event) {
    // NO Operation
  }

  @Override
  public void setContext(IEvictionContext context) {
    referenceTime = context.getReferenceTime();
    IEvictionContext prevContext = evictionContext;
    evictionContext = context;
    // compute window length adjustment (delta) to account for time drift
    if (context.getSlidingInterval() != null) {
      if (prevContext == null) {
        delta = Integer.MAX_VALUE; // consider all events for the initial window
      } else {
        delta = context.getReferenceTime() - prevContext.getReferenceTime()
            - context.getSlidingInterval();
        if (Math.abs(delta) > 100) {
          LOG.warning(String.format("Possible clock drift or long running computation in window; "
                  + "Previous eviction time: %f, current eviction time: %f",
              (double) prevContext.getReferenceTime(),
              (double) context.getReferenceTime()));
        }
      }
    }

  }

  @Override
  public String toString() {
    return "DurationEvictionPolicy{"
        + "referenceTime=" + referenceTime
        + ", delta=" + delta
        + ", windowLength=" + windowLength
        + '}';
  }
}

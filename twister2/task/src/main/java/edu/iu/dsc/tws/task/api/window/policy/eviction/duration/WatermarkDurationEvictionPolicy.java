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

import edu.iu.dsc.tws.task.api.window.api.Event;
import edu.iu.dsc.tws.task.api.window.constant.Action;

public class WatermarkDurationEvictionPolicy<T> extends DurationEvictionPolicy<T> {
  private final long lag;

  public WatermarkDurationEvictionPolicy(long winLength) {
    this(winLength, Long.MAX_VALUE);
  }

  public WatermarkDurationEvictionPolicy(long winLength, long lag) {
    super(winLength);
    this.lag = lag;
    referenceTime = 0L;
  }

  @Override
  public Action evict(Event<T> event) {
    if (evictionContext == null) {
      return Action.STOP;
    }

    long referenceTime = evictionContext.getReferenceTime();
    long diff = referenceTime - event.getTimeStamp();
    if (diff < -lag) {
      return Action.STOP;
    } else if (diff < 0) {
      return Action.KEEP;
    } else {
      return super.evict(event);
    }
  }

  @Override
  public String toString() {
    return "WatermarkDurationEvictionPolicy{"
        + "lag=" + lag
        + ", referenceTime=" + referenceTime
        + '}' + super.toString();
  }
}

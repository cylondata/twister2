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
package edu.iu.dsc.tws.task.window.policy.eviction.count;

import edu.iu.dsc.tws.task.window.api.Event;
import edu.iu.dsc.tws.task.window.api.IEvictionContext;
import edu.iu.dsc.tws.task.window.constant.Action;

public class WatermarkCountEvictionPolicy<T> extends CountEvictionPolicy<T> {

  private long referenceTime = 0;
  private long processedCount = 0L;
  private IEvictionContext evictionContext;

  public WatermarkCountEvictionPolicy(long count) {
    super(count);
  }

  @Override
  public Action evict(Event<T> event) {
    if (evictionContext == null) {
      return Action.STOP;
    }
    Action action;
    if (event.getTimeStamp() <= referenceTime && processedCount < currentCount.get()) {
      action = super.evict(event);
      if (action == Action.PROCESS) {
        ++processedCount;
      }
    } else {
      action = Action.KEEP;
    }
    return action;
  }

  @Override
  public void track(Event<T> event) {
    // NO IMPLEMENTATION
  }

  @Override
  public void setContext(IEvictionContext context) {
    this.evictionContext = context;
    referenceTime = context.getReferenceTime();
    if (context.getCurrentCount() != null) {
      currentCount.set(context.getCurrentCount());
    } else {
      currentCount.set(processedCount + context.getSlidingCount());
    }
    processedCount = 0;
  }

  @Override
  public String toString() {
    return "WatermarkCountEvictionPolicy{"
        + "referenceTime=" + referenceTime
        + ", processedCount=" + processedCount
        + ", evictionContext=" + evictionContext
        + '}' + super.toString();
  }
}

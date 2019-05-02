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
package edu.iu.dsc.tws.task.api.window.policy.eviction.count;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import edu.iu.dsc.tws.task.api.window.api.Event;
import edu.iu.dsc.tws.task.api.window.api.IEvictionContext;
import edu.iu.dsc.tws.task.api.window.api.IEvictionPolicy;
import edu.iu.dsc.tws.task.api.window.constant.Action;

public class CountEvictionPolicy<T> implements IEvictionPolicy<T> {

  private static final Logger LOG = Logger.getLogger(CountEvictionPolicy.class.getName());

  protected final long threshold;
  protected final AtomicLong currentCount;

  public CountEvictionPolicy(long count) {
    this.threshold = count;
    this.currentCount = new AtomicLong();
  }

  @Override
  public Action evict(Event<T> event) {
    while (true) {
      long curVal = currentCount.get();
      if (curVal > threshold) {
        if (currentCount.compareAndSet(curVal, curVal - 1)) {
          return Action.EXPIRE;
        }
      } else {
        break;
      }
    }
    return Action.PROCESS;
  }

  @Override
  public void track(Event<T> event) {
    if (!event.isWatermark()) {
      currentCount.incrementAndGet();
    }
  }

  @Override
  public void setContext(IEvictionContext context) {

  }

  @Override
  public String toString() {
    return "CountEvictionPolicy{"
        + "threshold=" + threshold
        + ", currentCount=" + currentCount
        + '}';
  }
}

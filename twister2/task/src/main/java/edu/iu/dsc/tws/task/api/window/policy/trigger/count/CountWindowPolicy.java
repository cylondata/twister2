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
package edu.iu.dsc.tws.task.api.window.policy.trigger.count;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import edu.iu.dsc.tws.task.api.window.api.Event;
import edu.iu.dsc.tws.task.api.window.api.IEvictionPolicy;
import edu.iu.dsc.tws.task.api.window.manage.IManager;
import edu.iu.dsc.tws.task.api.window.policy.trigger.IWindowingPolicy;

public class CountWindowPolicy<T> implements IWindowingPolicy<T> {

  private static final Logger LOG = Logger.getLogger(CountWindowPolicy.class.getName());

  private final long count;
  private final AtomicInteger currentCount;
  private final IManager manager;
  private final IEvictionPolicy<T> evictionPolicy;
  private boolean started;

  public CountWindowPolicy(long count, IManager manager, IEvictionPolicy<T> evictionPolicy) {
    this.count = count;
    this.manager = manager;
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
    if (started && !event.isWatermark()) {
      if (currentCount.incrementAndGet() >= count) {
        this.manager.onEvent();
      }
    }
  }

  @Override
  public void reset() {
    this.currentCount.set(0);
  }

  @Override
  public void start() {
    this.started = true;
  }

  @Override
  public void shutdown() {

  }

  @Override
  public String toString() {
    return "CountWindowPolicy{"
        + "count=" + count
        + ", currentCount=" + currentCount
        + ", manager=" + manager
        + ", evictionPolicy=" + evictionPolicy
        + ", started=" + started
        + '}';
  }
}

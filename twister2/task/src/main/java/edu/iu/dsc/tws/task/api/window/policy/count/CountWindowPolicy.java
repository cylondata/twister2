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
package edu.iu.dsc.tws.task.api.window.policy.count;

import java.util.concurrent.atomic.AtomicInteger;

import edu.iu.dsc.tws.task.api.window.api.Event;
import edu.iu.dsc.tws.task.api.window.api.IEvictionPolicy;
import edu.iu.dsc.tws.task.api.window.manage.IManager;
import edu.iu.dsc.tws.task.api.window.policy.IWindowingPolicy;

public class CountWindowPolicy<T> implements IWindowingPolicy<T> {

  private final int count;
  private final AtomicInteger currentCount;
  private final IManager manager;
  private final IEvictionPolicy<T> evictionPolicy;
  private boolean started;

  public CountWindowPolicy(int count, IManager manager, IEvictionPolicy<T> evictionPolicy) {
    this.count = count;
    this.manager = manager;
    this.evictionPolicy = evictionPolicy;
    this.currentCount = new AtomicInteger();
    this.started = false;
  }

  @Override
  public boolean validate() {
    return false;
  }

  @Override
  public String whyInvalid() {
    return null;
  }

  @Override
  public void track(Event<T> event) {

  }

  @Override
  public void reset() {

  }

  @Override
  public void start() {

  }

  @Override
  public void shutdown() {

  }
}

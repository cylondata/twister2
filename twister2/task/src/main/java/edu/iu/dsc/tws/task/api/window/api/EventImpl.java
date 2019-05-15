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
package edu.iu.dsc.tws.task.api.window.api;

import edu.iu.dsc.tws.task.api.IMessage;

public class EventImpl<T> implements Event<T> {

  private final IMessage<T> event;
  private long ts;

  public EventImpl(IMessage<T> event, long t) {
    this.event = event;
    this.ts = t;
  }

  @Override
  public long getTimeStamp() {
    return this.ts;
  }

  @Override
  public IMessage<T> get() {
    return this.event;
  }

  @Override
  public boolean isWatermark() {
    return false;
  }

  @Override
  public String toString() {
    return "EventImpl{"
        + "event=" + event
        + ", ts=" + ts
        + '}';
  }
}

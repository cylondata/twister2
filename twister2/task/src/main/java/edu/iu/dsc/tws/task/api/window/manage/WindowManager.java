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
package edu.iu.dsc.tws.task.api.window.manage;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.window.api.Event;
import edu.iu.dsc.tws.task.api.window.api.EventImpl;
import edu.iu.dsc.tws.task.api.window.api.IEvictionPolicy;
import edu.iu.dsc.tws.task.api.window.api.IWindowMessage;
import edu.iu.dsc.tws.task.api.window.api.WindowLifeCycleListener;
import edu.iu.dsc.tws.task.api.window.api.WindowMessageImpl;
import edu.iu.dsc.tws.task.api.window.constant.Action;
import edu.iu.dsc.tws.task.api.window.policy.trigger.IWindowingPolicy;

public class WindowManager<T> implements IManager<T> {

  private static final Logger LOG = Logger.getLogger(WindowManager.class.getName());

  private static final long serialVersionUID = -15452808832480739L;

  private IWindowingPolicy<T> windowingPolicy;

  private IEvictionPolicy<T> evictionPolicy;

  private WindowLifeCycleListener<T> windowLifeCycleListener;

  private List<IMessage<T>> expiredEvents;

  private ReentrantLock lock;

  private final ConcurrentLinkedQueue<Event<T>> queue;

  public WindowManager(WindowLifeCycleListener<T> windowLifeCycleListener) {
    this.windowLifeCycleListener = windowLifeCycleListener;
    this.queue = new ConcurrentLinkedQueue<>();
    this.expiredEvents = new ArrayList<>();
    this.lock = new ReentrantLock();
  }

  public WindowManager() {
    this.queue = new ConcurrentLinkedQueue<>();
    this.expiredEvents = new ArrayList<>();
    this.lock = new ReentrantLock();
  }

  public IWindowingPolicy<T> getWindowingPolicy() {
    return windowingPolicy;
  }

  public void setWindowingPolicy(IWindowingPolicy<T> windowingPolicy) {
    this.windowingPolicy = windowingPolicy;
  }

  public IEvictionPolicy<T> getEvictionPolicy() {
    return evictionPolicy;
  }

  public void setEvictionPolicy(IEvictionPolicy<T> evictionPolicy) {
    this.evictionPolicy = evictionPolicy;
  }

  @Override
  public void add(IMessage<T> message) {
    add(message, System.currentTimeMillis());
  }


  public void add(IMessage<T> message, long ts) {
    add(new EventImpl<T>(message, ts));
  }

  public void add(Event<T> windowEvent) {
    if (!windowEvent.isWatermark()) {
      queue.add(windowEvent);
    } else {
      LOG.info(String.format("Event With WaterMark ts %f ", (double) windowEvent.getTimeStamp()));
    }
    track(windowEvent);
  }


  @Override
  public void onEvent() {
    List<Event<T>> windowEvents = null;
    List<IMessage<T>> expired = null;
    try {
      lock.lock();
      windowEvents = scanEvents(true);
      expired = new ArrayList<>(expiredEvents);
      expiredEvents.clear();
    } finally {
      lock.unlock();
    }
    if (!windowEvents.isEmpty()) {
      IWindowMessage<T> iWindowMessage = bundleWindowMessage(windowEvents);
      this.windowLifeCycleListener.onActivation(iWindowMessage, null, null);
    }
    this.windowingPolicy.reset();
  }

  public List<Event<T>> scanEvents(boolean fullScan) {
    List<IMessage<T>> eventsToExpire = new ArrayList<>();
    List<Event<T>> eventsToProcess = new ArrayList<>();
    try {
      lock.lock();
      Iterator<Event<T>> it = queue.iterator();
      while (it.hasNext()) {
        Event<T> windowEvent = it.next();
        Action action = evictionPolicy.evict(windowEvent);
        if (action == Action.EXPIRE) {
          eventsToExpire.add(windowEvent.get());
          it.remove();
        } else if (!fullScan || action == Action.STOP) {
          break;
        } else if (action == Action.PROCESS) {
          eventsToProcess.add(windowEvent);
        }
      }
      expiredEvents.addAll(eventsToExpire);
    } finally {
      lock.unlock();
    }

    return eventsToProcess;
  }

  public IWindowMessage<T> bundleWindowMessage(List<Event<T>> events) {
    WindowMessageImpl winMessage = null;
    List<IMessage<T>> messages = new ArrayList<>();
    for (Event<T> event : events) {
      IMessage<T> m = event.get();
      messages.add(m);
    }
    winMessage = new WindowMessageImpl(messages);
    return winMessage;
  }


  public void track(Event<T> windowEvent) {
    this.evictionPolicy.track(windowEvent);
    this.windowingPolicy.track(windowEvent);
  }

  public void compactWindow() {
    //TODO : handle the expired window accumilation with caution
  }

  public void shutdown() {
    if (windowingPolicy != null) {
      windowingPolicy.shutdown();
    }
  }

}

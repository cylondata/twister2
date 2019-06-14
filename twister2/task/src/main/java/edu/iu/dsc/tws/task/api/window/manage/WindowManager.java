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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
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

  public static final int EXPIRE_EVENTS_THRESHOLD = 20;

  private static final long serialVersionUID = -15452808832480739L;

  private IWindowingPolicy<T> windowingPolicy;

  private IEvictionPolicy<T> evictionPolicy;

  private WindowLifeCycleListener<T> windowLifeCycleListener;

  private List<IMessage<T>> expiredEvents;

  private ReentrantLock lock;

  private final ConcurrentLinkedQueue<Event<T>> queue;

  private final Set<Event<T>> previousWindowEvents;

  private final AtomicInteger eventsSinceLastExpiration;

  public WindowManager(WindowLifeCycleListener<T> windowLifeCycleListener) {
    this.windowLifeCycleListener = windowLifeCycleListener;
    this.queue = new ConcurrentLinkedQueue<>();
    this.expiredEvents = new ArrayList<>();
    this.lock = new ReentrantLock();
    this.previousWindowEvents = new HashSet<>();
    this.eventsSinceLastExpiration = new AtomicInteger();
  }

  public WindowManager() {
    this.queue = new ConcurrentLinkedQueue<>();
    this.expiredEvents = new ArrayList<>();
    this.lock = new ReentrantLock();
    this.previousWindowEvents = new HashSet<>();
    this.eventsSinceLastExpiration = new AtomicInteger();
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
      LOG.fine(String.format("Event With WaterMark ts %f ", (double) windowEvent.getTimeStamp()));
    }
    track(windowEvent);
    compactWindow();
  }


  @Override
  public boolean onEvent() {
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
    List<IMessage<T>> events = new ArrayList<>();
    List<IMessage<T>> newEvents = new ArrayList<>();
    for (Event<T> event : windowEvents) {
      events.add(event.get());
      if (!previousWindowEvents.contains(event)) {
        newEvents.add(event.get());
      }
    }
    previousWindowEvents.clear();
    if (!events.isEmpty()) {
      previousWindowEvents.addAll(windowEvents);
      LOG.log(Level.FINE, String.format("WindowLifeCycleListener onActivation, "
          + "events in the window : %d", events.size()));
      IWindowMessage<T> ievents = bundleNonExpiredWindowIMessage(events);
      IWindowMessage<T> inewEvents = bundleNonExpiredWindowIMessage(newEvents);
      //TODO : handle expired events
      IWindowMessage<T> iexpired = bundleExpiredWindowIMessage(expired);
      windowLifeCycleListener.onActivation(ievents, inewEvents, iexpired);
    } else {
      LOG.log(Level.FINE,
          String.format("No events processed for the window, onActivation method is not called"));
    }

    this.windowingPolicy.reset();

    return !events.isEmpty();
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
    eventsSinceLastExpiration.set(0);
    if (!eventsToExpire.isEmpty()) {
      LOG.severe(String.format("OnExiry called on WindowLifeCycleListener"));
      IWindowMessage<T> eventsToExpireIWindow = bundleExpiredWindowIMessage(eventsToExpire);
      windowLifeCycleListener.onExpiry(eventsToExpireIWindow);
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

  /**
   * This method bundles data into a IWindowMessage for creating non-expired IWindowMessages
   * @param events list of elements that need to be passed into a window
   * @return a bundled IWindowMessage considering a list of IMessages of a given data type
   */
  public IWindowMessage<T> bundleNonExpiredWindowIMessage(List<IMessage<T>> events) {
    WindowMessageImpl winMessage = null;
    List<IMessage<T>> messages = new ArrayList<>();
    for (IMessage<T> m : events) {
      messages.add(m);
    }
    winMessage = new WindowMessageImpl(messages);
    return winMessage;
  }

  /**
   * This method bundles data into a IWindowMessage for creating expired IWindowMessages
   *@param events list of elements that need to be passed into a window
   * @return a bundled IWindowMessage considering a list of IMessages of a given data type
   */
  public IWindowMessage<T> bundleExpiredWindowIMessage(List<IMessage<T>> events) {
    WindowMessageImpl winMessage = null;
    List<IMessage<T>> messages = new ArrayList<>();
    for (IMessage<T> m : events) {
      messages.add(m);
    }
    winMessage = new WindowMessageImpl(null, messages);
    return winMessage;
  }


  public void track(Event<T> windowEvent) {
    this.evictionPolicy.track(windowEvent);
    this.windowingPolicy.track(windowEvent);
  }

  public void compactWindow() {
    if (eventsSinceLastExpiration.incrementAndGet() >= EXPIRE_EVENTS_THRESHOLD) {
      scanEvents(false);
    }
  }

  public void shutdown() {
    if (windowingPolicy != null) {
      windowingPolicy.shutdown();
    }
  }

  /**
   * This method is scanning the list of events falling under a given
   * starting and end time stamp
   *
   * @param start the starting timestamp
   * @param end the end time stamp
   * @param slide the sliding interval count
   * @return this list of events with time stamps
   */
  public List<Long> getSlidingCountTimestamps(long start, long end, long slide) {
    List<Long> timestamps = new ArrayList<>();
    if (end > start) {
      int count = 0;
      long ts = Long.MIN_VALUE;
      for (Event<T> event : queue) {
        if (event.getTimeStamp() > start && event.getTimeStamp() <= end) {
          ts = Math.max(ts, event.getTimeStamp());
          if (++count % slide == 0) {
            timestamps.add(ts);
          }
        }
      }
    }
    return timestamps;
  }

  /**
   * This method returns the number of event count which has the timestamp lesser than the
   * reference timestamp
   * @param referenceTime timestamp to which we compare the event time to filter them out
   * @return the number of event count which is less than or equal to the reference time
   */
  public long getEventCount(long referenceTime) {
    long eventCount = 0;
    for (Event<T> event : queue) {
      if (event.getTimeStamp() <= referenceTime) {
        ++eventCount;
      }
    }
    return eventCount;
  }

  /**
   * provides the event with earliest timestamp between end and start timestamps
   * by scanning through the queue of messages.
   *
   * @param start starting timestamp (excluded for calculation)
   * @param end ending timestamp (included for calculation)
   * @return the minimum timestamp the queue between start and end
   */
  public long getEarliestEventTimestamp(long start, long end) {
    long minTimestamp = Long.MAX_VALUE;
    for (Event<T> event : queue) {
      if (event.getTimeStamp() > start && event.getTimeStamp() <= end) {
        minTimestamp = Math.min(minTimestamp, event.getTimeStamp());
      }
    }
    return minTimestamp;
  }

}

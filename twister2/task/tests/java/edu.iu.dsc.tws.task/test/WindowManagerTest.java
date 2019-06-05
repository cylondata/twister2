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
package edu.iu.dsc.tws.task.test;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskMessage;
import edu.iu.dsc.tws.task.api.window.api.IEvictionPolicy;
import edu.iu.dsc.tws.task.api.window.api.IWindowMessage;
import edu.iu.dsc.tws.task.api.window.api.WindowLifeCycleListener;
import edu.iu.dsc.tws.task.api.window.api.WindowMessageImpl;
import edu.iu.dsc.tws.task.api.window.manage.WindowManager;
import edu.iu.dsc.tws.task.api.window.policy.eviction.count.CountEvictionPolicy;
import edu.iu.dsc.tws.task.api.window.policy.trigger.IWindowingPolicy;
import edu.iu.dsc.tws.task.api.window.policy.trigger.count.CountWindowPolicy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WindowManagerTest {

  private WindowManager<Integer> windowManager;
  private Listener listener;

  private static class Listener implements WindowLifeCycleListener<Integer> {
    private IWindowMessage<Integer> onExpiryEvents = new WindowMessageImpl<>(null, null);
    private IWindowMessage<Integer> onActivationEvents = new WindowMessageImpl<>(null, null);
    private IWindowMessage<Integer> onActivationNewEvents = new WindowMessageImpl<>(null, null);
    private IWindowMessage<Integer> onActivationExpiredEvents = new WindowMessageImpl<>(null, null);

    // all events since last clear
    private List<IWindowMessage<Integer>> allOnExpiryEvents = new ArrayList<>();
    private List<IWindowMessage<Integer>> allOnActivationEvents = new ArrayList<>();
    private List<IWindowMessage<Integer>> allOnActivationNewEvents = new ArrayList<>();
    private List<IWindowMessage<Integer>> allOnActivationExpiredEvents = new ArrayList<>();

    public void clear() {
      onExpiryEvents = new WindowMessageImpl<>(null, null);
      onActivationEvents = new WindowMessageImpl<>(null, null);
      onActivationNewEvents = new WindowMessageImpl<>(null, null);
      onActivationExpiredEvents = new WindowMessageImpl<>(null, null);

      allOnExpiryEvents.clear();
      allOnActivationEvents.clear();
      allOnActivationNewEvents.clear();
      allOnActivationExpiredEvents.clear();
    }

    @Override
    public void onExpiry(IWindowMessage<Integer> events) {
      onExpiryEvents = events;
      allOnActivationEvents.add(events);
    }

    @Override
    public void onActivation(IWindowMessage<Integer> events, IWindowMessage<Integer> newEvents,
                             IWindowMessage<Integer> expired) {
      onActivationEvents = events;
      allOnActivationEvents.add(events);
      onActivationNewEvents = newEvents;
      allOnActivationNewEvents.add(newEvents);
      onActivationExpiredEvents = expired;
      allOnActivationExpiredEvents.add(expired);
    }
  }

  @Before
  public void setUp() {
    listener = new Listener();
    windowManager = new WindowManager<>(listener);
  }

  @After
  public void tearDown() {
    windowManager.shutdown();
  }

  @Test
  public void testCountBasedWindow() throws Exception {
    IEvictionPolicy<Integer> evictionPolicy = new CountEvictionPolicy<Integer>(5);
    IWindowingPolicy<Integer> triggerPolicy = new CountWindowPolicy<>(2, windowManager,
        evictionPolicy);
    triggerPolicy.start();
    windowManager.setEvictionPolicy(evictionPolicy);
    windowManager.setWindowingPolicy(triggerPolicy);
    IMessage<Integer> m1 = new TaskMessage<>(1);
    IMessage<Integer> m2 = new TaskMessage<>(2);
    windowManager.add(m1);
    windowManager.add(m2);
    // nothing expired yet
    assertTrue(listener.onExpiryEvents.getExpiredWindow().isEmpty());
    assertEquals(seqIMessage(m1, m2), listener.onActivationEvents);
//    assertEquals(seq(1, 2), listener.onActivationNewEvents);
//    assertTrue(listener.onActivationExpiredEvents.isEmpty());
//    windowManager.add(3);
//    windowManager.add(4);
//    // nothing expired yet
//    assertTrue(listener.onExpiryEvents.isEmpty());
//    assertEquals(seq(1, 4), listener.onActivationEvents);
//    assertEquals(seq(3, 4), listener.onActivationNewEvents);
//    assertTrue(listener.onActivationExpiredEvents.isEmpty());
//    windowManager.add(5);
//    windowManager.add(6);
//    // 1 expired
//    assertEquals(seq(1), listener.onExpiryEvents);
//    assertEquals(seq(2, 6), listener.onActivationEvents);
//    assertEquals(seq(5, 6), listener.onActivationNewEvents);
//    assertEquals(seq(1), listener.onActivationExpiredEvents);
//    listener.clear();
//    windowManager.add(7);
//    // nothing expires until threshold is hit
//    assertTrue(listener.onExpiryEvents.isEmpty());
//    windowManager.add(8);
//    // 1 expired
//    assertEquals(seq(2, 3), listener.onExpiryEvents);
//    assertEquals(seq(4, 8), listener.onActivationEvents);
//    assertEquals(seq(7, 8), listener.onActivationNewEvents);
//    assertEquals(seq(2, 3), listener.onActivationExpiredEvents);
  }

  private List<Integer> seq(int start, int stop) {
    List<Integer> ints = new ArrayList<>();
    for (int i = start; i <= stop; i++) {
      ints.add(i);
    }
    return ints;
  }

  private List<IMessage<Integer>> seqIMessage(IMessage<Integer> start, IMessage<Integer> end) {
    List<IMessage<Integer>> iMessageList = new ArrayList<>();
    for (int i = start.getContent(); i < end.getContent(); i++) {
      iMessageList.add(new TaskMessage(i));
    }
    return iMessageList;
  }

}

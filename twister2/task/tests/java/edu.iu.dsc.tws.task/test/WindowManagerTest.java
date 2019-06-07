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
import static org.junit.Assert.*;

public class WindowManagerTest {

  private WindowManager<Integer> windowManager;
  private Listener listener;

  private List<IMessage<Integer>> mockList;

  private static class Listener implements WindowLifeCycleListener<Integer> {
    private IWindowMessage<Integer> onExpiryEvents
        = new WindowMessageImpl<>(new ArrayList<IMessage<Integer>>(),
        new ArrayList<IMessage<Integer>>());
    private IWindowMessage<Integer> onActivationEvents
        = new WindowMessageImpl<>(new ArrayList<IMessage<Integer>>(),
        new ArrayList<IMessage<Integer>>());
    private IWindowMessage<Integer> onActivationNewEvents
        = new WindowMessageImpl<>(new ArrayList<IMessage<Integer>>(),
        new ArrayList<IMessage<Integer>>());
    private IWindowMessage<Integer> onActivationExpiredEvents
        = new WindowMessageImpl<>(new ArrayList<IMessage<Integer>>(),
        new ArrayList<IMessage<Integer>>());

    // all events since last clear
    private List<IWindowMessage<Integer>> allOnExpiryEvents = new ArrayList<>();
    private List<IWindowMessage<Integer>> allOnActivationEvents = new ArrayList<>();
    private List<IWindowMessage<Integer>> allOnActivationNewEvents = new ArrayList<>();
    private List<IWindowMessage<Integer>> allOnActivationExpiredEvents = new ArrayList<>();

    public void clear() {
      onExpiryEvents = new WindowMessageImpl<>(new ArrayList<IMessage<Integer>>(),
          new ArrayList<IMessage<Integer>>());
      onActivationEvents = new WindowMessageImpl<>(new ArrayList<IMessage<Integer>>(),
          new ArrayList<IMessage<Integer>>());
      onActivationNewEvents = new WindowMessageImpl<>(new ArrayList<IMessage<Integer>>(),
          new ArrayList<IMessage<Integer>>());
      onActivationExpiredEvents = new WindowMessageImpl<>(new ArrayList<IMessage<Integer>>(),
          new ArrayList<IMessage<Integer>>());

      allOnExpiryEvents.clear();
      allOnActivationEvents.clear();
      allOnActivationNewEvents.clear();
      allOnActivationExpiredEvents.clear();
    }

    @Override
    public void onExpiry(IWindowMessage<Integer> events) {
      onExpiryEvents = events;
      allOnExpiryEvents.add(events);
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
    IMessage<Integer> m0 = new TaskMessage<>(0);
    IMessage<Integer> m1 = new TaskMessage<>(1);
    IMessage<Integer> m2 = new TaskMessage<>(2);
    IMessage<Integer> m3 = new TaskMessage<>(3);
    IMessage<Integer> m4 = new TaskMessage<>(4);
    IMessage<Integer> m5 = new TaskMessage<>(5);
    IMessage<Integer> m6 = new TaskMessage<>(6);
    mockList = new ArrayList<>(7);
    mockList.add(m0);
    mockList.add(m1);
    mockList.add(m2);
    mockList.add(m3);
    mockList.add(m4);
    mockList.add(m5);
    mockList.add(m6);

    windowManager.add(m0);
    windowManager.add(m1);
    // nothing expired yet
    assertTrue(listener.onExpiryEvents.getExpiredWindow().isEmpty());

    assertEquals(null, null);
    int start = 0;
    int end = 1;
    List<IMessage<Integer>> num01 = seqIMessage(start, end);
    assertEquals(num01.size(), (end - start) + 1);

    int count = 0;

    printIWindowMessage(listener.onActivationEvents, new IOutputFunction<Integer>() {
      private int count = 0;

      @Override
      public void print(Integer integer) {
        System.out.println(count++ + " : " + integer);
      }
    });


    printIMessageList(num01, new IOutputFunction<Integer>() {
      private int count = 0;

      @Override
      public void print(Integer integer) {
        System.out.println(count++ + " : " + integer);
      }
    });


    assertTrue(listener.onExpiryEvents.getExpiredWindow().isEmpty());
    assertEquals(seqIMessage(0, 1), listener.onActivationEvents.getWindow());
    assertNotEquals(seqIMessage(1, 2), listener.onActivationEvents.getWindow());
    assertEquals(seqIMessage(0, 1), listener.onActivationNewEvents.getWindow());
    //assertTrue(listener.onActivationExpiredEvents.getExpiredWindow().isEmpty());

  }

  private List<Integer> seq(int start, int stop) {
    List<Integer> ints = new ArrayList<>();
    for (int i = start; i <= stop; i++) {
      ints.add(i);
    }
    return ints;
  }

  private List<IMessage<Integer>> seqIMessage(int start, int end) {
    if (mockList.size() >= (end - start) + 1) {
      List<IMessage<Integer>> iMessageList = new ArrayList<>();
      for (int i = start; i <= end; i++) {
        iMessageList.add(mockList.get(i));
      }

      return iMessageList;

    }
    return null;
  }

  private <T> void printIWindowMessage(IWindowMessage<T> windowMessage,
                                       IOutputFunction<T> outputfunction) {
    if (windowMessage.getWindow() != null) {
      printIMessageList(windowMessage.getWindow(), outputfunction);
    }
  }

  interface IOutputFunction<T> {
    void print(T t);
  }

  private <T> void printIMessageList(List<IMessage<T>> iMessageList,
                                     IOutputFunction<T> outputFunction) {
    if (iMessageList != null && outputFunction != null) {
      for (IMessage<T> message : iMessageList) {
        printIMessage(message, outputFunction);
      }
    }
  }

  private <T> void printIMessage(IMessage<T> message, IOutputFunction<T> outputFunction) {
    outputFunction.print(message.getContent());
  }

}

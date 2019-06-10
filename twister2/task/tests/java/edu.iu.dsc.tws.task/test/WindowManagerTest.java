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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskMessage;
import edu.iu.dsc.tws.task.api.window.api.IEvictionPolicy;
import edu.iu.dsc.tws.task.api.window.api.IWindowMessage;
import edu.iu.dsc.tws.task.api.window.api.WindowLifeCycleListener;
import edu.iu.dsc.tws.task.api.window.api.WindowMessageImpl;
import edu.iu.dsc.tws.task.api.window.config.WindowConfig;
import edu.iu.dsc.tws.task.api.window.event.WatermarkEvent;
import edu.iu.dsc.tws.task.api.window.manage.WindowManager;
import edu.iu.dsc.tws.task.api.window.policy.eviction.count.CountEvictionPolicy;
import edu.iu.dsc.tws.task.api.window.policy.eviction.count.WatermarkCountEvictionPolicy;
import edu.iu.dsc.tws.task.api.window.policy.eviction.duration.WatermarkDurationEvictionPolicy;
import edu.iu.dsc.tws.task.api.window.policy.trigger.IWindowingPolicy;
import edu.iu.dsc.tws.task.api.window.policy.trigger.count.CountWindowPolicy;
import edu.iu.dsc.tws.task.api.window.policy.trigger.count.WatermarkCountWindowPolicy;
import edu.iu.dsc.tws.task.api.window.policy.trigger.duration.DurationWindowPolicy;
import edu.iu.dsc.tws.task.api.window.policy.trigger.duration.WatermarkDurationWindowPolicy;
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
    System.out.println(String.format("Before"));
    listener = new Listener();
    windowManager = new WindowManager<>(listener);
    generateAutoMockData();
  }

  @After
  public void tearDown() {
    windowManager.shutdown();
  }


  public void generateMockData() {
    IMessage<Integer> m0 = new TaskMessage<>(0);
    IMessage<Integer> m1 = new TaskMessage<>(1);
    IMessage<Integer> m2 = new TaskMessage<>(2);
    IMessage<Integer> m3 = new TaskMessage<>(3);
    IMessage<Integer> m4 = new TaskMessage<>(4);
    IMessage<Integer> m5 = new TaskMessage<>(5);
    IMessage<Integer> m6 = new TaskMessage<>(6);
    IMessage<Integer> m7 = new TaskMessage<>(7);
    IMessage<Integer> m8 = new TaskMessage<>(8);
    IMessage<Integer> m9 = new TaskMessage<>(9);
    IMessage<Integer> m10 = new TaskMessage<>(10);
    mockList = new ArrayList<>(10);
    mockList.add(m0);
    mockList.add(m1);
    mockList.add(m2);
    mockList.add(m3);
    mockList.add(m4);
    mockList.add(m5);
    mockList.add(m6);
    mockList.add(m7);
    mockList.add(m8);
    mockList.add(m9);
    mockList.add(m10);
  }

  public void generateAutoMockData() {
    final int size = WindowManager.EXPIRE_EVENTS_THRESHOLD * 5;
    mockList = new ArrayList<>(size);
    for (int i = 0; i <= size; i++) {
      mockList.add(new TaskMessage<Integer>(i));
    }
  }

  @Test
  public void testCountBasedWindow() throws Exception {
    IEvictionPolicy<Integer> evictionPolicy = new CountEvictionPolicy<Integer>(5);
    IWindowingPolicy<Integer> triggerPolicy = new CountWindowPolicy<>(2, windowManager,
        evictionPolicy);
    triggerPolicy.start();
    windowManager.setEvictionPolicy(evictionPolicy);
    windowManager.setWindowingPolicy(triggerPolicy);


    windowManager.add(mockList.get(0));
    windowManager.add(mockList.get(1));
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
    assertTrue(listener.onActivationExpiredEvents.getExpiredWindow().isEmpty());

    windowManager.add(mockList.get(2));
    windowManager.add(mockList.get(3));

    assertTrue(listener.onExpiryEvents.getExpiredWindow().isEmpty());

    assertEquals(seqIMessage(0, 3), listener.onActivationEvents.getWindow());
    assertEquals(seqIMessage(2, 3), listener.onActivationNewEvents.getWindow());
    assertTrue(listener.onActivationExpiredEvents.getExpiredWindow().isEmpty());
    windowManager.add(mockList.get(4));
    windowManager.add(mockList.get(5));

    // 1 expired ( here the expected window size exceeds and expiration calls on eviction)
    assertEquals(seqIMessage(0), listener.onExpiryEvents.getExpiredWindow());
    assertEquals(seqIMessage(1, 5), listener.onActivationEvents.getWindow());
    assertEquals(seqIMessage(4, 5), listener.onActivationNewEvents.getWindow());
    assertEquals(seqIMessage(0), listener.onActivationExpiredEvents.getExpiredWindow());
    listener.clear();
    windowManager.add(mockList.get(6));
    // nothing expires until threshold is hit
    assertTrue(listener.onExpiryEvents.getExpiredWindow().isEmpty());
    windowManager.add(mockList.get(7));
    // 1 expired ( here the expected window size exceeds and expiration calls on eviction)
    assertEquals(seqIMessage(1, 2), listener.onExpiryEvents.getExpiredWindow());
    assertEquals(seqIMessage(3, 7), listener.onActivationEvents.getWindow());
    assertEquals(seqIMessage(6, 7), listener.onActivationNewEvents.getWindow());
    assertEquals(seqIMessage(1, 2), listener.onActivationExpiredEvents.getExpiredWindow());

  }


  @Test
  public void testTumblingWindow() throws Exception {
    IEvictionPolicy<Integer> evictionPolicy = new CountEvictionPolicy<Integer>(3);
    windowManager.setEvictionPolicy(evictionPolicy);
    IWindowingPolicy<Integer> triggerPolicy = new CountWindowPolicy<Integer>(3,
        windowManager, evictionPolicy);
    triggerPolicy.start();
    windowManager.setWindowingPolicy(triggerPolicy);
    windowManager.add(mockList.get(0));
    windowManager.add(mockList.get(1));
    // nothing expired yet
    assertTrue(listener.onExpiryEvents.getExpiredWindow().isEmpty());
    windowManager.add(mockList.get(2));
    assertTrue(listener.onExpiryEvents.getExpiredWindow().isEmpty());
    assertEquals(seqIMessage(0, 2), listener.onActivationEvents.getWindow());
    assertTrue(listener.onActivationExpiredEvents.getExpiredWindow().isEmpty());
    assertEquals(seqIMessage(0, 2), listener.onActivationNewEvents.getWindow());

    listener.clear();
    windowManager.add(mockList.get(3));
    windowManager.add(mockList.get(4));
    windowManager.add(mockList.get(5));

    assertEquals(seqIMessage(0, 2), listener.onExpiryEvents.getExpiredWindow());
    assertEquals(seqIMessage(3, 5), listener.onActivationEvents.getWindow());
    assertEquals(seqIMessage(0, 2), listener.onActivationExpiredEvents.getExpiredWindow());
    assertEquals(seqIMessage(3, 5), listener.onActivationNewEvents.getWindow());

  }

  @Test
  public void testEventTimeBasedWindow() throws Exception {
    IEvictionPolicy<Integer> evictionPolicy = new WatermarkDurationEvictionPolicy<>(20);
    windowManager.setEvictionPolicy(evictionPolicy);
    IWindowingPolicy<Integer> triggerPolicy
        = new WatermarkDurationWindowPolicy<Integer>(10, windowManager, windowManager,
        evictionPolicy);
    triggerPolicy.start();
    windowManager.setWindowingPolicy(triggerPolicy);

    windowManager.add(mockList.get(0), 603);
    windowManager.add(mockList.get(1), 605);
    windowManager.add(mockList.get(2), 607);

    // This should trigger the scan to find
    // the next aligned window end ts, but not produce any activations
    windowManager.add(new WatermarkEvent<>(609));
    assertEquals(Collections.emptyList(), listener.allOnActivationEvents);

    windowManager.add(mockList.get(3), 618);
    windowManager.add(mockList.get(4), 626);
    windowManager.add(mockList.get(5), 636);
    // send a watermark event, which should trigger three windows.
    windowManager.add(new WatermarkEvent<>(631));

//        System.out.println(listener.allOnActivationEvents);
    assertEquals(3, listener.allOnActivationEvents.size());
    assertEquals(mockList.subList(0, 3), listener.allOnActivationEvents.get(0).getWindow());
    assertEquals(mockList.subList(0, 4), listener.allOnActivationEvents.get(1).getWindow());
    assertEquals(mockList.subList(3, 5), listener.allOnActivationEvents.get(2).getWindow());

    assertEquals(Collections.emptyList(), listener.allOnActivationExpiredEvents.get(0)
        .getExpiredWindow());
    assertEquals(Collections.emptyList(), listener.allOnActivationExpiredEvents.get(1)
        .getExpiredWindow());
    assertEquals(mockList.subList(0, 3), listener.allOnActivationExpiredEvents.get(2)
        .getExpiredWindow());

    assertEquals(mockList.subList(0, 3), listener.allOnActivationNewEvents.get(0).getWindow());
    assertEquals(mockList.subList(3, 4), listener.allOnActivationNewEvents.get(1).getWindow());
    assertEquals(mockList.subList(4, 5), listener.allOnActivationNewEvents.get(2).getWindow());

    assertEquals(mockList.subList(0, 3), listener.allOnExpiryEvents.get(0).getExpiredWindow());

    // add more events with a gap in ts
    windowManager.add(mockList.get(6), 825);
    windowManager.add(mockList.get(7), 826);
    windowManager.add(mockList.get(8), 827);
    windowManager.add(mockList.get(9), 839);

    listener.clear();
    windowManager.add(new WatermarkEvent<Integer>(834));

    assertEquals(3, listener.allOnActivationEvents.size());
    assertEquals(mockList.subList(4, 6), listener.allOnActivationEvents.get(0).getWindow());
    assertEquals(mockList.subList(5, 6), listener.allOnActivationEvents.get(1).getWindow());
    assertEquals(mockList.subList(6, 9), listener.allOnActivationEvents.get(2).getWindow());

    assertEquals(mockList.subList(3, 4), listener.allOnActivationExpiredEvents.get(0)
        .getExpiredWindow());
    assertEquals(mockList.subList(4, 5), listener.allOnActivationExpiredEvents.get(1)
        .getExpiredWindow());
    assertEquals(Collections.emptyList(), listener.allOnActivationExpiredEvents.get(2)
        .getExpiredWindow());

    assertEquals(mockList.subList(5, 6), listener.allOnActivationNewEvents.get(0).getWindow());
    assertEquals(Collections.emptyList(), listener.allOnActivationNewEvents.get(1).getWindow());
    assertEquals(mockList.subList(6, 9), listener.allOnActivationNewEvents.get(2).getWindow());

    assertEquals(mockList.subList(3, 4), listener.allOnExpiryEvents.get(0).getExpiredWindow());
    assertEquals(mockList.subList(4, 5), listener.allOnExpiryEvents.get(1).getExpiredWindow());
    assertEquals(mockList.subList(5, 6), listener.allOnExpiryEvents.get(2).getExpiredWindow());
  }

  @Test
  public void testCountBasedWindowWithEventTs() throws Exception {
    IEvictionPolicy<Integer> evictionPolicy = new WatermarkCountEvictionPolicy<>(3);
    windowManager.setEvictionPolicy(evictionPolicy);
    IWindowingPolicy<Integer> triggerPolicy =
        new WatermarkDurationWindowPolicy<Integer>(10, windowManager, windowManager,
            evictionPolicy);
    triggerPolicy.start();
    windowManager.setWindowingPolicy(triggerPolicy);

    windowManager.add(mockList.get(0), 603);
    windowManager.add(mockList.get(1), 605);
    windowManager.add(mockList.get(2), 607);
    windowManager.add(mockList.get(3), 618);
    windowManager.add(mockList.get(4), 626);
    windowManager.add(mockList.get(5), 636);
    // send a watermark event, which should trigger three windows.
    windowManager.add(new WatermarkEvent<>(631));

    assertEquals(3, listener.allOnActivationEvents.size());
    assertEquals(mockList.subList(0, 3), listener.allOnActivationEvents.get(0).getWindow());
    assertEquals(mockList.subList(1, 4), listener.allOnActivationEvents.get(1).getWindow());
    assertEquals(mockList.subList(2, 5), listener.allOnActivationEvents.get(2).getWindow());

    // add more events with a gap in ts
    windowManager.add(mockList.get(6), 665);
    windowManager.add(mockList.get(7), 666);
    windowManager.add(mockList.get(8), 667);
    windowManager.add(mockList.get(9), 679);

    listener.clear();
    windowManager.add(new WatermarkEvent<>(674));
//        System.out.println(listener.allOnActivationEvents);
    assertEquals(4, listener.allOnActivationEvents.size());
    // same set of events part of three windows
    assertEquals(mockList.subList(3, 6), listener.allOnActivationEvents.get(0).getWindow());
    assertEquals(mockList.subList(3, 6), listener.allOnActivationEvents.get(1).getWindow());
    assertEquals(mockList.subList(3, 6), listener.allOnActivationEvents.get(2).getWindow());
    assertEquals(mockList.subList(6, 9), listener.allOnActivationEvents.get(3).getWindow());
  }

  @Test
  public void testCountBasedTriggerWithEventTs() throws Exception {
    IEvictionPolicy<Integer> evictionPolicy = new WatermarkDurationEvictionPolicy<>(20);
    windowManager.setEvictionPolicy(evictionPolicy);
    IWindowingPolicy<Integer> triggerPolicy
        = new WatermarkCountWindowPolicy<>(3, windowManager, evictionPolicy, windowManager);
    triggerPolicy.start();
    windowManager.setWindowingPolicy(triggerPolicy);

    windowManager.add(mockList.get(0), 603);
    windowManager.add(mockList.get(1), 605);
    windowManager.add(mockList.get(2), 607);
    windowManager.add(mockList.get(3), 618);
    windowManager.add(mockList.get(4), 625);
    windowManager.add(mockList.get(5), 626);
    windowManager.add(mockList.get(6), 629);
    windowManager.add(mockList.get(7), 636);
    // send a watermark event, which should trigger three windows.
    windowManager.add(new WatermarkEvent<>(631));
//        System.out.println(listener.allOnActivationEvents);

    assertEquals(2, listener.allOnActivationEvents.size());
    assertEquals(mockList.subList(0, 3), listener.allOnActivationEvents.get(0).getWindow());
    assertEquals(mockList.subList(2, 6), listener.allOnActivationEvents.get(1).getWindow());

    // add more events with a gap in ts
    windowManager.add(mockList.get(8), 665);
    windowManager.add(mockList.get(9), 666);
    windowManager.add(mockList.get(10), 667);
    windowManager.add(mockList.get(11), 669);
    windowManager.add(mockList.get(11), 679);

    listener.clear();
    windowManager.add(new WatermarkEvent<>(674));
//        System.out.println(listener.allOnActivationEvents);
    assertEquals(2, listener.allOnActivationEvents.size());
    // same set of events part of three windows
    assertEquals(mockList.get(8), listener.allOnActivationEvents.get(0).getWindow().get(0));
    assertEquals(mockList.subList(8, 12), listener.allOnActivationEvents.get(1).getWindow());
  }

  @Test
  public void testCountBasedTumblingWithSameEventTs() throws Exception {
    IEvictionPolicy<Integer> evictionPolicy = new WatermarkCountEvictionPolicy<>(2);
    windowManager.setEvictionPolicy(evictionPolicy);
    IWindowingPolicy<Integer> triggerPolicy
        = new WatermarkCountWindowPolicy<>(2, windowManager, evictionPolicy, windowManager);
    triggerPolicy.start();
    windowManager.setWindowingPolicy(triggerPolicy);

    windowManager.add(mockList.get(0), 10);
    windowManager.add(mockList.get(1), 10);
    windowManager.add(mockList.get(2), 11);
    windowManager.add(mockList.get(3), 12);
    windowManager.add(mockList.get(4), 12);
    windowManager.add(mockList.get(5), 12);
    windowManager.add(mockList.get(6), 12);
    windowManager.add(mockList.get(7), 13);
    windowManager.add(mockList.get(8), 14);
    windowManager.add(mockList.get(9), 15);

    windowManager.add(new WatermarkEvent<>(20));
    assertEquals(5, listener.allOnActivationEvents.size());
    assertEquals(mockList.subList(0, 2), listener.allOnActivationEvents.get(0).getWindow());
    assertEquals(mockList.subList(2, 4), listener.allOnActivationEvents.get(1).getWindow());
    assertEquals(mockList.subList(4, 6), listener.allOnActivationEvents.get(2).getWindow());
    assertEquals(mockList.subList(6, 8), listener.allOnActivationEvents.get(3).getWindow());
    assertEquals(mockList.subList(8, 10), listener.allOnActivationEvents.get(4).getWindow());
  }

  //TODO : the test expired threshold must be tested, test cases fail
  private void testExpireThreshold() throws Exception {
    int threshold = WindowManager.EXPIRE_EVENTS_THRESHOLD;
    int windowLength = 5;

    // init 1
//    IEvictionPolicy<Integer> evictionPolicy = new CountEvictionPolicy<Integer>(5);
//    IWindowingPolicy<Integer> windowingPolicy
//        = new DurationWindowPolicy<Integer>(new WindowConfig
//        .Duration(1, TimeUnit.HOURS).value, windowManager, evictionPolicy);
//    windowManager.setEvictionPolicy(evictionPolicy);
//    windowManager.setWindowingPolicy(windowingPolicy);
//    windowingPolicy.start();

    //init 2
    IEvictionPolicy<Integer> evictionPolicy = new CountEvictionPolicy<Integer>(5);
    IWindowingPolicy<Integer> windowingPolicy
        = new DurationWindowPolicy<Integer>(
        new WindowConfig
            .Duration(1, TimeUnit.HOURS).value, windowManager, evictionPolicy);
    windowManager.setEvictionPolicy(evictionPolicy);
    windowManager.setWindowingPolicy(windowingPolicy);

    windowingPolicy.start();

    for (IMessage<Integer> i : mockList.subList(0, 4)) {
      windowManager.add(i);
    }


    // nothing expired yet
    assertTrue(listener.onExpiryEvents.getExpiredWindow().isEmpty());
    for (IMessage<Integer> i : mockList.subList(5, 9)) {
      windowManager.add(i);
    }
    for (IMessage<Integer> i : mockList.subList(10, threshold)) {
      windowManager.add(i);
    }

    // window should be compacted and events should be expired.
    List<IMessage<Integer>> seqM = mockList.subList(0, threshold - windowLength);
    System.out.println("---------------");
    System.out.println();
    printIMessageList(seqM, new IOutputFunction<Integer>() {
      @Override
      public void print(Integer integer) {
        System.out.print(integer + " ");
      }
    });
    System.out.println();
    System.out.println("---------------");
    System.out.println();
    printIMessageList(listener.onExpiryEvents.getExpiredWindow(), new IOutputFunction<Integer>() {
      @Override
      public void print(Integer integer) {
        System.out.print(integer + " ");
      }
    });
    System.out.println();
    System.out.println("---------------");
    assertEquals(seqM, listener.onExpiryEvents.getExpiredWindow());
  }

  private void testEvictBeforeWatermarkForWatermarkEvictionPolicy(
      IEvictionPolicy<Integer> watermarkEvictionPolicy, int windowLength) throws Exception {
    /**
     * The watermark eviction policy must not evict tuples until the first watermark has been received.
     * The policies can't make a meaningful decision prior to the first watermark, so the safe decision
     * is to postpone eviction.
     */
    int threshold = WindowManager.EXPIRE_EVENTS_THRESHOLD;
    windowManager.setEvictionPolicy(watermarkEvictionPolicy);
    WatermarkCountWindowPolicy<Integer> windowingPolicy
        = new WatermarkCountWindowPolicy(windowLength, windowManager,
        watermarkEvictionPolicy, windowManager);
    windowManager.setWindowingPolicy(windowingPolicy);
    windowingPolicy.start();

    for (IMessage<Integer> i : seqIMessage(1, threshold)) {
      windowManager.add(i, i.getContent());
    }
//    assertThat("The watermark eviction policies should never evict " +
//            "events before the first watermark is received",
//        listener.onExpiryEvents.getExpiredWindow(), CoreMatchers.is(empty()));
    windowManager.add(new WatermarkEvent<>(threshold));
    // The events should be put in a window when the first watermark is received
    assertEquals(seqIMessage(0, threshold), listener.onActivationEvents.getWindow());
    //Now add some more events and a new watermark, and check that the previous events are expired
    for (IMessage<Integer> i : seqIMessage(threshold + 1, threshold * 2)) {
      windowManager.add(i, i.getContent());
    }
    windowManager.add(new WatermarkEvent<Integer>(threshold + windowLength + 1));
    //All the events should be expired when the next watermark is received
    assertEquals(listener.onExpiryEvents.getExpiredWindow(), seqIMessage(0, threshold));

  }

  //TODO : test expire threshold must be tested
  private void testExpireThresholdWithWatermarkCountEvictionPolicy() throws Exception {
    int windowLength = WindowManager.EXPIRE_EVENTS_THRESHOLD;
    IEvictionPolicy watermarkCountEvictionPolicy = new WatermarkCountEvictionPolicy(windowLength);
    testEvictBeforeWatermarkForWatermarkEvictionPolicy(watermarkCountEvictionPolicy, windowLength);
  }

  //TODO : test expire threshold must be tested
  private void testExpireThresholdWithWatermarkTimeEvictionPolicy() throws Exception {
    int windowLength = WindowManager.EXPIRE_EVENTS_THRESHOLD;
    IEvictionPolicy<Integer> watermarkTimeEvictionPolicy
        = new WatermarkDurationEvictionPolicy<>(windowLength);
    testEvictBeforeWatermarkForWatermarkEvictionPolicy(watermarkTimeEvictionPolicy, windowLength);
  }

  private List<Integer> seq(int start, int stop) {
    List<Integer> ints = new ArrayList<>();
    for (int i = start; i <= stop; i++) {
      ints.add(i);
    }
    return ints;
  }

  private List<IMessage<Integer>> seqIMessage(int start) {
    return seqIMessage(start, start);
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

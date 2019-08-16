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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.iu.dsc.tws.task.window.api.Event;
import edu.iu.dsc.tws.task.window.api.GlobalStreamId;
import edu.iu.dsc.tws.task.window.event.WatermarkEventGenerator;
import edu.iu.dsc.tws.task.window.manage.WindowManager;
import static org.junit.Assert.*;

public class WatermarkGeneratorTest {

  private WatermarkEventGenerator<Integer> watermarkEventGenerator;
  private WindowManager<Integer> windowManager;
  private List<Event<Integer>> eventList = new ArrayList<>();

  private GlobalStreamId streamId(String component) {
    return new GlobalStreamId(component);
  }

  @Before
  public void setUp() {
    System.out.println("Before");

    windowManager = new WindowManager<Integer>(null) {
      @Override
      public void add(Event<Integer> event) {
        eventList.add(event);
      }
    };
    // set watermark interval to a high value and trigger manually to fix timing issues
    watermarkEventGenerator = new WatermarkEventGenerator(windowManager, 5, 100000,
        Collections.singleton(streamId("s1")));
    watermarkEventGenerator.start();
  }

  @After
  public void tearDown() {
    System.out.println("After");
    watermarkEventGenerator.shutdown();
  }

  @Test
  public void testTrackSingleStream() throws Exception {
    watermarkEventGenerator.track(streamId("s1"), 100);
    watermarkEventGenerator.track(streamId("s1"), 110);
    watermarkEventGenerator.run();
    assertTrue(eventList.get(0).isWatermark());
    assertEquals(105, eventList.get(0).getTimeStamp());
  }

  @Test
  public void testTrackSingleStreamOutOfOrder() throws Exception {
    watermarkEventGenerator.track(streamId("s1"), 100);
    watermarkEventGenerator.track(streamId("s1"), 110);
    watermarkEventGenerator.track(streamId("s1"), 104);
    watermarkEventGenerator.run();
    assertTrue(eventList.get(0).isWatermark());
    assertEquals(105, eventList.get(0).getTimeStamp());
  }


  @Test
  public void testTrackTwoStreams() throws Exception {
    Set<GlobalStreamId> streams = new HashSet<>();
    streams.add(streamId("s1"));
    streams.add(streamId("s2"));
    watermarkEventGenerator = new WatermarkEventGenerator<>(windowManager, 5, 10000, streams);

    watermarkEventGenerator.track(streamId("s1"), 100);
    watermarkEventGenerator.track(streamId("s1"), 110);
    watermarkEventGenerator.run();
    assertTrue(eventList.isEmpty());
    watermarkEventGenerator.track(streamId("s2"), 95);
    watermarkEventGenerator.track(streamId("s2"), 98);
    watermarkEventGenerator.run();
    assertTrue(eventList.get(0).isWatermark());
    assertEquals(93, eventList.get(0).getTimeStamp());
  }

  @Test
  public void testNoEvents() throws Exception {
    watermarkEventGenerator.run();
    assertTrue(eventList.isEmpty());
  }

  @Test
  public void testLateEvent() throws Exception {
    assertTrue(watermarkEventGenerator.track(streamId("s1"), 100));
    assertTrue(watermarkEventGenerator.track(streamId("s1"), 110));
    watermarkEventGenerator.run();
    assertTrue(eventList.get(0).isWatermark());
    assertEquals(105, eventList.get(0).getTimeStamp());
    eventList.clear();
    assertTrue(watermarkEventGenerator.track(streamId("s1"), 105));
    assertTrue(watermarkEventGenerator.track(streamId("s1"), 106));
    assertTrue(watermarkEventGenerator.track(streamId("s1"), 115));
    assertFalse(watermarkEventGenerator.track(streamId("s1"), 104));
    watermarkEventGenerator.run();
    assertTrue(eventList.get(0).isWatermark());
    assertEquals(110, eventList.get(0).getTimeStamp());
  }

}

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
package edu.iu.dsc.tws.common.pojo;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * TimeTest Contains the test cases for the Time class used in Windowing
 * Running these tests using bazel
 * bazel test --config=ubuntu twister2/common/src/tests/java:TimeTest
 */
public class TimeTest {

  private Time sampleTimeMilli = null;
  private Time sampleTimeSeconds = null;
  private Time sampleTimeMinutes = null;
  private Time sampleTimeHours = null;
  private Time sampleTimeDays = null;

  private final long period = 100;

  @Before
  public void setup() {
    this.sampleTimeMilli = new Time(period, TimeUnit.MILLISECONDS);
    this.sampleTimeSeconds = new Time(period, TimeUnit.SECONDS);
    this.sampleTimeMinutes = new Time(period, TimeUnit.MINUTES);
    this.sampleTimeHours = new Time(period, TimeUnit.HOURS);
    this.sampleTimeDays = new Time(period, TimeUnit.DAYS);
  }

  @Test
  public void toMilliseconds() {
    assertEquals(new Time(period, TimeUnit.MILLISECONDS), sampleTimeMilli);
    assertEquals(new Time(period, TimeUnit.SECONDS), sampleTimeSeconds);
    assertEquals(new Time(period, TimeUnit.MINUTES), sampleTimeMinutes);
    assertEquals(new Time(period, TimeUnit.HOURS), sampleTimeHours);
    assertEquals(new Time(period, TimeUnit.DAYS), sampleTimeDays);
  }

  @Test
  public void of() {
    assertEquals(Time.of(period, TimeUnit.MILLISECONDS), sampleTimeMilli);
    assertEquals(Time.of(period, TimeUnit.SECONDS), sampleTimeSeconds);
    assertEquals(Time.of(period, TimeUnit.MINUTES), sampleTimeMinutes);
    assertEquals(Time.of(period, TimeUnit.HOURS), sampleTimeHours);
    assertEquals(Time.of(period, TimeUnit.DAYS), sampleTimeDays);
  }

  @Test
  public void milliseconds() {
    assertEquals(Time.milliseconds(period), sampleTimeMilli);

  }

  @Test
  public void seconds() {
    assertEquals(Time.seconds(period), sampleTimeSeconds);
  }

  @Test
  public void minutes() {
    assertEquals(Time.minutes(period), sampleTimeMinutes);
  }

  @Test
  public void hours() {
    assertEquals(Time.hours(period), sampleTimeHours);
  }

  @Test
  public void days() {
    assertEquals(Time.days(period), sampleTimeDays);
  }
}

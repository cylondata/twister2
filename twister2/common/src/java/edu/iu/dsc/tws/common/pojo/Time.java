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

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class Time implements Serializable {

  private final long size;

  private final TimeUnit unit;

  /**
   * Initializes Time
   * @param size time period
   * @param unit TimeUnit type
   */
  public Time(long size, TimeUnit unit) {
    this.size = size;
    this.unit = unit;
  }

  /**
   * Returns size of the time unit
   * @return size of the time
   */
  public long getSize() {
    return size;
  }

  /**
   * Returns the TimeUnit type
   * @return TimeUnit
   */
  public TimeUnit getUnit() {
    return unit;
  }

  /**
   * Returns the TimeUnit value in Milliseconds
   * @return time into milliseconds
   */
  public long toMilliseconds() {
    return unit.toMillis(size);
  }

  @Override
  public String toString() {
    return toMilliseconds() + " ms";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Time time = (Time) o;
    return toMilliseconds() == time.toMilliseconds();
  }

  @Override
  public int hashCode() {
    return Objects.hash(toMilliseconds());
  }

  /**
   * Set Time with length and TimeUnit
   * @param size length of the time duration
   * @param unit TimeUnit type
   * @return Time Object
   */
  public static Time of(long size, TimeUnit unit) {
    return new Time(size, unit);
  }

  /**
   * Set Time with length with milliseconds
   * @param milliseconds Time as milliseconds
   * @return
   */
  public static Time milliseconds(long milliseconds) {
    return of(milliseconds, TimeUnit.MILLISECONDS);
  }

  /**
   * Set Time with length and seconds
   * @param seconds Time as seconds
   * @return
   */
  public static Time seconds(long seconds) {
    return of(seconds, TimeUnit.SECONDS);
  }

  /**
   * Set Time with length and minutes
   * @param minutes Time as minutes
   * @return
   */
  public static Time minutes(long minutes) {
    return of(minutes, TimeUnit.MINUTES);
  }

  /**
   * Set Time with length and hours
   * @param hours Time as hours
   * @return
   */
  public static Time hours(long hours) {
    return of(hours, TimeUnit.HOURS);
  }


  /**
   * Set Time with length and days
   * @param days Time as days
   * @return
   */
  public static Time days(long days) {
    return of(days, TimeUnit.DAYS);
  }
}

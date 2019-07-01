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
package edu.iu.dsc.tws.task.window.util;


import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import edu.iu.dsc.tws.task.window.config.WindowConfig;

public class WindowParameter implements Serializable {

  /**
   * Window Type Description
   * Window Length == Sliding Length => Tumbling Window
   * Window Length  > Sliding Length => Sliding Window
   * Window Length  < Sliding Length => Invalid Window
   */

  private WindowConfig.Count windowCountSize;

  private WindowConfig.Count slidingCountSize;

  private WindowConfig.Duration windowDurationSize;

  private WindowConfig.Duration sldingDurationSize;

  /**
   * Tumbling
   */
  public WindowParameter withTumblingCountWindow(long winLength) {
    return withTumblingCountWindowInit(winLength);
  }

  private WindowParameter withTumblingCountWindowInit(long winLength) {
    this.windowCountSize = newCountInstance(winLength);
    this.slidingCountSize = this.windowCountSize;
    return this;
  }

  public WindowParameter withTumblingDurationWindow(long winLength, TimeUnit timeUnit) {
    return withTumblingDurationWindowInit(winLength, timeUnit);
  }

  private WindowParameter withTumblingDurationWindowInit(long winLength, TimeUnit timeUnit) {
    this.windowDurationSize = newDurationInstance(winLength, timeUnit);
    this.sldingDurationSize = this.windowDurationSize;
    return this;
  }

  /**
   * Sliding
   */

  public WindowParameter withSlidingingCountWindow(long winLength, long slidingLength) {
    return withSlidingCountWindowInit(winLength, slidingLength);
  }

  private WindowParameter withSlidingCountWindowInit(long winLength, long slidingLength) {
    this.windowCountSize = newCountInstance(winLength);
    this.slidingCountSize = newCountInstance(slidingLength);
    return this;
  }

  public WindowParameter withSlidingDurationWindow(long winLength, TimeUnit winLengthTU,
                                                   long slidingLength, TimeUnit slidingLengthTU) {
    return withSlidingDurationWindowInit(winLength, winLengthTU, slidingLength, slidingLengthTU);
  }

  private WindowParameter withSlidingDurationWindowInit(long winLength, TimeUnit winLengthTU,
                                                        long slidingLength,
                                                        TimeUnit slidingLengthTU) {
    this.windowDurationSize = newDurationInstance(winLength, winLengthTU);
    this.sldingDurationSize = newDurationInstance(slidingLength, slidingLengthTU);
    return this;
  }


  private WindowConfig.Count newCountInstance(long value) {
    return new WindowConfig.Count(value);
  }

  private WindowConfig.Duration newDurationInstance(long value, TimeUnit timeUnit) {
    return new WindowConfig.Duration(value, timeUnit);
  }

  public WindowConfig.Count getWindowCountSize() {
    return windowCountSize;
  }

  public WindowConfig.Count getSlidingCountSize() {
    return slidingCountSize;
  }

  public WindowConfig.Duration getWindowDurationSize() {
    return windowDurationSize;
  }

  public WindowConfig.Duration getSldingDurationSize() {
    return sldingDurationSize;
  }
}

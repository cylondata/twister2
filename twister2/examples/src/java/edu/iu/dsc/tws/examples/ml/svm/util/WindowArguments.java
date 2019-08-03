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
package edu.iu.dsc.tws.examples.ml.svm.util;

import java.io.Serializable;
import java.util.Objects;

import edu.iu.dsc.tws.task.window.constant.WindowType;

public class WindowArguments implements Serializable {

  private static final long serialVersionUID = 7215545889817090308L;
  private WindowType windowType;

  private long windowLength;

  private long slidingLength;

  private boolean isDuration;

  public WindowArguments(WindowType windowType, long windowLength, long slidingLength,
                         boolean isDuration) {
    this.windowType = windowType;
    this.windowLength = windowLength;
    this.slidingLength = slidingLength;
    this.isDuration = isDuration;
  }

  public WindowType getWindowType() {
    return windowType;
  }

  public void setWindowType(WindowType windowType) {
    this.windowType = windowType;
  }

  public long getWindowLength() {
    return windowLength;
  }

  public void setWindowLength(long windowLength) {
    this.windowLength = windowLength;
  }

  public long getSlidingLength() {
    return slidingLength;
  }

  public void setSlidingLength(long slidingLength) {
    this.slidingLength = slidingLength;
  }

  public boolean isDuration() {
    return isDuration;
  }

  public void setDuration(boolean duration) {
    isDuration = duration;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WindowArguments that = (WindowArguments) o;
    return windowLength == that.windowLength
        && slidingLength == that.slidingLength
        && isDuration == that.isDuration
        && windowType == that.windowType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(windowType, windowLength, slidingLength, isDuration);
  }

  @Override
  public String toString() {
    return "WindowArguments{"
        + "windowType=" + windowType
        + ", windowLength=" + windowLength
        + ", slidingLength=" + slidingLength
        + ", isDuration=" + isDuration
        + '}';
  }
}

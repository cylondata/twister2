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
package edu.iu.dsc.tws.task.api.window.api;

public abstract class BaseWindow implements IWindow {

  protected final long windowLength;
  protected final long slideLength;

  public BaseWindow(long windowLength, long slideLength) {
    this.windowLength = windowLength;
    this.slideLength = slideLength;
  }

  @Override
  public long getWindowLength() {
    return this.windowLength;
  }

  @Override
  public long getSlidingLength() {
    return this.slideLength;
  }

  @Override
  public void validate() {
    if (slideLength > windowLength) {
      throw new IllegalArgumentException("sliding length '" + slideLength
          + "' must always be less than windowing length '" + windowLength + "'");
    }
  }
}

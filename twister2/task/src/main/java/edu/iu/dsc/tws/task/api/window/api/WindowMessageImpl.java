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

import java.util.List;
import java.util.Objects;

import edu.iu.dsc.tws.task.api.IMessage;

public class WindowMessageImpl<T> implements IWindowMessage<T> {

  private List<IMessage<T>> currentWindow;

  private List<IMessage<T>> expiredWindow;

  public WindowMessageImpl(List<IMessage<T>> currentWindow) {
    this.currentWindow = currentWindow;
  }


  public WindowMessageImpl(List<IMessage<T>> currentWindow, List<IMessage<T>> expiredWindow) {
    this.currentWindow = currentWindow;
    this.expiredWindow = expiredWindow;
  }

  @Override
  public List<IMessage<T>> getWindow() {
    return this.currentWindow;
  }

  @Override
  public List<IMessage<T>> getExpiredWindow() {
    return this.expiredWindow;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof WindowMessageImpl)) {
      return false;
    }
    WindowMessageImpl<?> that = (WindowMessageImpl<?>) o;
    return Objects.equals(currentWindow, that.currentWindow)
        && Objects.equals(getExpiredWindow(), that.getExpiredWindow());
  }

  @Override
  public String toString() {
    return "WindowMessageImpl{"
        + "currentWindow=" + currentWindow
        + ", expiredWindow=" + expiredWindow
        + '}';
  }

  @Override
  public int hashCode() {

    return Objects.hash(currentWindow, getExpiredWindow());
  }
}

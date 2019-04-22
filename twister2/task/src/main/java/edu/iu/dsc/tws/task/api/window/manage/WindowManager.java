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
package edu.iu.dsc.tws.task.api.window.manage;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.task.api.window.config.WindowConfig;
import edu.iu.dsc.tws.task.api.window.constant.Window;
import edu.iu.dsc.tws.task.api.window.policy.WindowingPolicy;

public class WindowManager<T> implements IManager<T> {

  private static final Logger LOG = Logger.getLogger(WindowManager.class.getName());

  private static final long serialVersionUID = -15452808832480739L;

  private WindowingPolicy windowingPolicy;

  private List<T> windowedObjects;

  private int windowCountSize = 0;

  private WindowConfig.Duration windowDurationSize;

  private boolean windowingCompleted = false;

  public WindowManager(WindowingPolicy windowingPolicy) {
    this.windowingPolicy = windowingPolicy;
    this.windowingPolicy = initializeWindowingPolicy();
  }

  /**
   * TODO : Windowing Graph must be considered and policy has to be arranged
   * When multiple windowing policies are applied, they need to be handled sequentially
   */
  @Override
  public WindowingPolicy initializeWindowingPolicy() {

    if (this.windowingPolicy.getWindow().equals(Window.TUMBLING)) {
      if (this.windowingPolicy.getCount().value > 0) {
        this.windowedObjects = new ArrayList<T>(this.windowingPolicy.getCount().value);
      }
      //TODO : implement the timing based one
    }

    if (this.windowingPolicy.getWindow().equals(Window.SLIDING)) {
      // TODO : implement the logic
    }

    if (this.windowingPolicy.getWindow().equals(Window.SESSION)) {
      // TODO : implement the logic
    }

    if (this.windowingPolicy.getWindow().equals(Window.GLOBAL)) {
      // TODO : implement the logic
    }

    return this.windowingPolicy;
  }

  @Override
  public List<T> getWindow() {
    return this.windowedObjects;
  }

  @Override
  public boolean execute(T message) {

    boolean status = false;
    if (progress(this.windowedObjects)) {
      if (this.windowingPolicy.getWindow().equals(Window.TUMBLING)) {
        windowCountSize = this.windowingPolicy.getCount().value;
        windowDurationSize = this.windowingPolicy.getDuration();
        if (windowCountSize > 0 && this.windowedObjects.size() < windowCountSize) {
          this.windowedObjects.add(message);
          status = true;
        }
        //TODO : implement the duration logic
      }

      if (this.windowingPolicy.getWindow().equals(Window.SLIDING)) {
        // TODO : implement the logic
      }

      if (this.windowingPolicy.getWindow().equals(Window.SESSION)) {
        // TODO : implement the logic
      }

      if (this.windowingPolicy.getWindow().equals(Window.GLOBAL)) {
        // TODO : implement the logic
      }

    }
    return status;
  }

  @Override
  public void clearWindow() {
    this.windowingCompleted = false;
    this.windowedObjects.clear();
  }

  @Override
  public boolean progress(List<T> window) {
    boolean progress = true;
    if (this.windowingPolicy.getWindow().equals(Window.TUMBLING)) {
      windowCountSize = this.windowingPolicy.getCount().value;
      windowDurationSize = this.windowingPolicy.getDuration();
      if (window.size() == windowCountSize && windowCountSize > 0) {
        progress = false;
      }
      // TODO : implement duration logic
    }

    if (this.windowingPolicy.getWindow().equals(Window.SLIDING)) {
      // TODO : implement the logic
    }

    if (this.windowingPolicy.getWindow().equals(Window.SESSION)) {
      // TODO : implement the logic
    }

    if (this.windowingPolicy.getWindow().equals(Window.GLOBAL)) {
      // TODO : implement the logic
    }

    return progress;
  }

  @Override
  public boolean isDone() {
    if (this.windowingPolicy.getWindow().equals(Window.TUMBLING)) {
      windowCountSize = this.windowingPolicy.getCount().value;
      windowDurationSize = this.windowingPolicy.getDuration();
      if (this.windowedObjects.size() == windowCountSize && windowCountSize > 0) {
        this.windowingCompleted = true;
      } else {
        this.windowingCompleted = false;
      }
      // TODO : implement duration logic
    }

    if (this.windowingPolicy.getWindow().equals(Window.SLIDING)) {
      // TODO : implement the logic
    }

    if (this.windowingPolicy.getWindow().equals(Window.SESSION)) {
      // TODO : implement the logic
    }

    if (this.windowingPolicy.getWindow().equals(Window.GLOBAL)) {
      // TODO : implement the logic
    }
    return this.windowingCompleted;
  }
}

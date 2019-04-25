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

import edu.iu.dsc.tws.task.api.window.api.IWindowMessage;
import edu.iu.dsc.tws.task.api.window.api.WindowMessageImpl;
import edu.iu.dsc.tws.task.api.window.config.WindowConfig;
import edu.iu.dsc.tws.task.api.window.constant.WindowType;
import edu.iu.dsc.tws.task.api.window.policy.WindowingPolicy;

public class WindowManager<T> implements IManager<T> {

  private static final Logger LOG = Logger.getLogger(WindowManager.class.getName());

  private static final long serialVersionUID = -15452808832480739L;

  private WindowingPolicy windowingPolicy;

  private List<T> windowedObjects;

  private int windowCountSize = 0;

  private WindowMessageImpl<T> windowMessageImpl;

  private IWindowMessage<T> windowMessage;

  private WindowConfig.Duration windowDurationSize;

  private boolean windowingCompleted = false;

  public WindowManager() {
  }

  public WindowManager(WindowingPolicy windowingPolicy) {
    this.windowingPolicy = windowingPolicy;
    this.windowingPolicy = initializeWindowingPolicy();

  }

  /**
   * Calls when a single policy is passed via the constructor
   * When multiple windowing policies are applied, they need to be handled sequentially
   */
  @Override
  public WindowingPolicy initializeWindowingPolicy() {
    this.addWindowingPolicy(this.windowingPolicy);
    return this.windowingPolicy;
  }


  /**
   * Adding windowing policy to a map.
   * Each added policy is added to the windowing policy map with a unique id for each policy.
   *
   * @param win WindowingPolicy
   */
  @Override
  public WindowingPolicy addWindowingPolicy(WindowingPolicy win) {
    this.windowingPolicy = win;
    this.windowedObjects = new ArrayList<>();
    return win;
  }

  @Override
  public List<T> getWindow() {
    return this.windowedObjects;
  }

  public IWindowMessage<T> getWindowMessage() {
    return this.windowMessage;
  }

  /**
   * This method process the input message from the task instance and packages the policy
   * TODO : Move packaging logic to WindowingPolicyManager to provide a portable model [C-logic]
   * TODO : This portability allows the user to write their own windowing rules
   * TODO : Handle expired messages : expire visibility (how many previous windows [user decides])
   *
   * @param message Input message from SinkStreamingWindowInstance
   */
  @Override
  public boolean execute(T message) {
    boolean status = false;
    WindowingPolicy w = this.windowingPolicy;
    if (progress(this.windowedObjects)) {
      if (this.windowingPolicy.getWindowType().equals(WindowType.TUMBLING)) {
        int winCount = w.getCount().value;
        WindowConfig.Duration duration = windowDurationSize = w.getDuration();
        if (winCount > 0 && this.windowedObjects.size() < winCount) {
          this.windowedObjects.add(message);
          status = true;
        }
        //TODO : implement the duration logic
      }

      if (w.getWindowType().equals(WindowType.SLIDING)) {
        // TODO : implement the logic
      }

      if (w.getWindowType().equals(WindowType.SESSION)) {
        // TODO : implement the logic
      }

      if (w.getWindowType().equals(WindowType.GLOBAL)) {
        // TODO : implement the logic
      }
    }

    return status;
  }

  /**
   * Clear the windowed message list per windowing policy once a staged windowing is done
   */
  @Override
  public void clearWindow() {
    this.windowingCompleted = false;
    this.windowedObjects.clear();
  }

  /**
   * This method is used to decide window packaging is progressed or not   *
   */
  @Override
  public boolean progress(List<T> window) {
    boolean progress = true;
    if (this.windowingPolicy.getWindowType().equals(WindowType.TUMBLING)) {
      windowCountSize = this.windowingPolicy.getCount().value;
      windowDurationSize = this.windowingPolicy.getDuration();
      if (window.size() == windowCountSize && windowCountSize > 0) {
        progress = false;
      }
      // TODO : implement duration logic
    }

    if (this.windowingPolicy.getWindowType().equals(WindowType.SLIDING)) {
      // TODO : implement the logic
    }

    if (this.windowingPolicy.getWindowType().equals(WindowType.SESSION)) {
      // TODO : implement the logic
    }

    if (this.windowingPolicy.getWindowType().equals(WindowType.GLOBAL)) {
      // TODO : implement the logic
    }

    return progress;
  }


  /**
   * This method is used to check whether the per stage windowing is completed or not
   */
  @Override
  public boolean isDone() {
    if (this.windowingPolicy.getWindowType().equals(WindowType.TUMBLING)) {
      windowCountSize = this.windowingPolicy.getCount().value;
      windowDurationSize = this.windowingPolicy.getDuration();
      if (this.windowedObjects.size() == windowCountSize && windowCountSize > 0) {
        //TODO : handle the expired tuples
        this.windowMessage = new WindowMessageImpl(this.windowedObjects, null);
        this.windowingCompleted = true;
      } else {
        this.windowingCompleted = false;
      }
      // TODO : implement duration logic
    }

    if (this.windowingPolicy.getWindowType().equals(WindowType.SLIDING)) {
      // TODO : implement the logic
    }

    if (this.windowingPolicy.getWindowType().equals(WindowType.SESSION)) {
      // TODO : implement the logic
    }

    if (this.windowingPolicy.getWindowType().equals(WindowType.GLOBAL)) {
      // TODO : implement the logic
    }
    return this.windowingCompleted;
  }
}

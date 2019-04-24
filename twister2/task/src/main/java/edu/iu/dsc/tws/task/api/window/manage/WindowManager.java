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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.task.api.window.api.IWindowMessage;
import edu.iu.dsc.tws.task.api.window.api.WindowMessageImpl;
import edu.iu.dsc.tws.task.api.window.config.WindowConfig;
import edu.iu.dsc.tws.task.api.window.constant.Window;
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

  private Map<Integer, WindowingPolicy> windowingPolicyMap = new HashMap<>();

  private Map<Integer, List<T>> mapOfPolicyIdAndWindows = new HashMap<>();

  private Map<Integer, Boolean> mapOfCompletedWindowsByStage = new HashMap<>();

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
   * @param win WindowingPolicy
   * @return
   */
  @Override
  public WindowingPolicy addWindowingPolicy(WindowingPolicy win) {
    int size = this.windowingPolicyMap.size();
    int key = -1;
    if (size == 0) {
      key = 0;
    } else {
      key = size;
    }
    this.windowingPolicyMap.put(key, win);
    this.mapOfPolicyIdAndWindows.put(key, new ArrayList<T>());
    this.mapOfCompletedWindowsByStage.put(key, false);
//    LOG.info(String.format("Windowing Policy Added [%d] : Policy Count : %d ",
//        key, this.windowingPolicyMap.size()));
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
   * @param message Input message from SinkStreamingWindowInstance
   * @return
   */
  @Override
  public boolean execute(T message) {
    boolean status = false;
    //TODO : do progress by checking the content in the policymap.
    for (Map.Entry<Integer, WindowingPolicy> e : windowingPolicyMap.entrySet()) {
      WindowingPolicy w = e.getValue();
      List<T> m = mapOfPolicyIdAndWindows.get(e.getKey());
      if (progress(w, m)) {
        if (w.getWindow().equals(Window.TUMBLING)) {
          int winCount = w.getCount().value;
          WindowConfig.Duration duration = windowDurationSize = w.getDuration();
          if (winCount > 0 && m.size() < winCount) {
            m.add(message);
            status = true;
          }
          //TODO : implement the duration logic
        }

        if (w.getWindow().equals(Window.SLIDING)) {
          // TODO : implement the logic
        }

        if (w.getWindow().equals(Window.SESSION)) {
          // TODO : implement the logic
        }

        if (w.getWindow().equals(Window.GLOBAL)) {
          // TODO : implement the logic
        }
      }
    }
    return status;
  }

  /**
   * Clear the windowed message list per windowing policy once a staged windowing is done
   *
   */
  @Override
  public void clearWindow() {
    for (Map.Entry<Integer, Boolean> e : mapOfCompletedWindowsByStage.entrySet()) {
      int key = e.getKey();
      boolean value = e.getValue();
      if (value) {
        mapOfPolicyIdAndWindows.get(key).clear();
        mapOfCompletedWindowsByStage.put(key, false);
      }
    }
  }

  /**
   * This method is used to decide window packaging is progressed or not
   * @deprecated this method is depreicated
   * @param window
   * @return
   */
  @Override
  @Deprecated
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

  /**
   * This method allows to decide whether to continue fulflilling the policy or stop
   * @param win Windowing Policy
   * @param window Windowing messages
   * @return
   */
  @Override
  public boolean progress(WindowingPolicy win, List<T> window) {
    boolean progress = true;
    if (win.getWindow().equals(Window.TUMBLING)) {
      int winCountSize = win.getCount().value;
      WindowConfig.Duration winDuration = win.getDuration();
      if (window.size() == winCountSize && winCountSize > 0) {
        progress = false;
      }
      // TODO : implement duration logic
    }

    if (win.getWindow().equals(Window.SLIDING)) {
      // TODO : implement the logic
    }

    if (win.getWindow().equals(Window.SESSION)) {
      // TODO : implement the logic
    }

    if (win.getWindow().equals(Window.GLOBAL)) {
      // TODO : implement the logic
    }

    return progress;
  }


  /**
   * Currently the multiple windowing policies are fired by OR logic
   * TODO : Analyze how to aggregate multiple policies
   * @return
   */
  public boolean isComplete() {
    boolean status = false;
    for (Map.Entry<Integer, WindowingPolicy> e : windowingPolicyMap.entrySet()) {
      int key = e.getKey();
      WindowingPolicy w = e.getValue();
      List<T> m = mapOfPolicyIdAndWindows.get(e.getKey());
      status = isDone(w, m);
      if (status) {
        this.windowMessage = new WindowMessageImpl(m, null);
        LOG.info(String.format("Completed [Key,Size] : [%d,%d]", key, m.size()));
        this.mapOfCompletedWindowsByStage.put(key, true);
      }
    }
    return status;
  }

  /**
   * This method checks a given windowing policy is fulfilled
   * This check is done per each stage when the expected window packaging is done
   * @param win Windowing policy
   * @param window Windowing messages
   * @return
   */
  @Override
  public boolean isDone(WindowingPolicy win, List<T> window) {
    boolean status = false;
    if (win.getWindow().equals(Window.TUMBLING)) {
      int winCountSize = win.getCount().value;
      WindowConfig.Duration winDuration = win.getDuration();
      if (window.size() == winCountSize && winCountSize > 0) {
        status = true;
      } else {
        status = false;
      }
      // TODO : implement duration logic
    }

    if (win.getWindow().equals(Window.SLIDING)) {
      // TODO : implement the logic
    }

    if (win.getWindow().equals(Window.SESSION)) {
      // TODO : implement the logic
    }

    if (win.getWindow().equals(Window.GLOBAL)) {
      // TODO : implement the logic
    }

    return status;
  }

  /**
   * This method is used to check whether the per stage windowing is completed or not
   * @deprecated this method is deprecated
   * @return
   */
  @Override
  @Deprecated
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

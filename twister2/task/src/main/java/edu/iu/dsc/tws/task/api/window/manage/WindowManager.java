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

import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.window.api.IWindowMessage;
import edu.iu.dsc.tws.task.api.window.api.WindowMessageImpl;
import edu.iu.dsc.tws.task.api.window.config.WindowConfig;
import edu.iu.dsc.tws.task.api.window.exceptions.InValidWindowingPolicy;
import edu.iu.dsc.tws.task.api.window.policy.IWindowingPolicy;
import edu.iu.dsc.tws.task.api.window.policy.WindowingTumblingPolicy;

public class WindowManager<T> implements IManager<T> {

  private static final Logger LOG = Logger.getLogger(WindowManager.class.getName());

  private static final long serialVersionUID = -15452808832480739L;

  private IWindowingPolicy windowingPolicy;

  private WindowingPolicyManager<T> windowingPolicyManager;

  private List<IMessage<T>> windowedObjects;

  private int windowCountSize = 0;

  private WindowConfig.Duration windowDurationSize;

  private IWindowMessage<T> windowMessage;

  private boolean windowingCompleted = false;

  public WindowManager() {
  }

  public WindowManager(IWindowingPolicy windowingPolicy) throws InValidWindowingPolicy {
    this.windowingPolicy = windowingPolicy;
    this.windowingPolicy = initializeWindowingPolicy();

  }

  /**
   * Calls when a single policy is passed via the constructor
   * When multiple windowing policies are applied, they need to be handled sequentially
   */
  @Override
  public IWindowingPolicy initializeWindowingPolicy() throws InValidWindowingPolicy {
    if (windowingPolicy.validate()) {
      this.addWindowingPolicy(this.windowingPolicy);
      if (windowingPolicy instanceof WindowingTumblingPolicy) {
        windowingPolicyManager = new WindowingTumblingPolicyManager<>();
        windowingPolicyManager.initialize(windowingPolicy);
      }
    } else {
      throw new InValidWindowingPolicy(String.format("Invalid Windowing Policy Included : %s ",
          windowingPolicy.whyInvalid()));
    }
    return this.windowingPolicy;
  }


  /**
   * Adding windowing policy to a map.
   * Each added policy is added to the windowing policy map with a unique id for each policy.
   *
   * @param win WindowingPolicy
   */
  @Override
  public IWindowingPolicy addWindowingPolicy(IWindowingPolicy win) {
    this.windowingPolicy = win;
    this.windowedObjects = new ArrayList<>();
    return win;
  }

  public IWindowMessage<T> getWindowMessage() {
    return this.windowMessage;
  }

  /**
   * This method process the input message from the task instance and packages the policy
   * TODO : Handle expired messages : expire visibility (how many previous windows [user decides])
   *
   * @param message Input message from SinkStreamingWindowInstance
   */
  @Override
  public boolean execute(IMessage<T> message) {
    boolean status = false;
    if (progress(this.windowedObjects)) {
      windowingPolicyManager.execute(message);
      this.windowedObjects = windowingPolicyManager.getWindows();
      status = true;
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
    windowingPolicyManager.clearWindow();
  }

  /**
   * This method is used to decide window packaging is progressed or not
   */
  @Override
  public boolean progress(List<IMessage<T>> window) {
    boolean progress = true;
    windowCountSize = windowingPolicy.getCount().value;
    windowDurationSize = windowingPolicy.getDuration();
    if (window.size() == windowCountSize && windowCountSize > 0) {
      progress = false;
    }
    return progress;
  }


  /**
   * This method is used to check whether the per stage windowing is completed or not
   */
  @Override
  public boolean isDone() {
    windowCountSize = windowingPolicy.getCount().value;
    windowDurationSize = windowingPolicy.getDuration();
    if (this.windowedObjects.size() == windowCountSize && windowCountSize > 0) {
      //TODO : handle the expired tuples
      this.windowMessage = new WindowMessageImpl(this.windowedObjects, null);
      this.windowingCompleted = true;
    } else {
      this.windowingCompleted = false;
    }
    return this.windowingCompleted;
  }
}

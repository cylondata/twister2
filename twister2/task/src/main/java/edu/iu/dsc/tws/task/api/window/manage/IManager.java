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

import java.io.Serializable;
import java.util.List;

import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.window.policy.IWindowingPolicy;

public interface IManager<T> extends Serializable {

  IWindowingPolicy initializeWindowingPolicy();

  IWindowingPolicy addWindowingPolicy(IWindowingPolicy windowingPolicy);

  boolean execute(IMessage<T> message);

  void clearWindow();

  boolean progress(List<IMessage<T>> window);

//  boolean progress(WindowingPolicy windowingPolicy, List<T> window);

  boolean isDone();

//  boolean isDone(WindowingPolicy windowingPolicy, List<T> window);

}

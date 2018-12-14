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
package edu.iu.dsc.tws.api.tset.ops;

import edu.iu.dsc.tws.api.tset.Selector;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.api.TaskKeySelector;

public class TaskKeySelectorImpl<T, K> implements TaskKeySelector {
  private Selector<T, K> selector;

  public TaskKeySelectorImpl(Selector<T, K> selec) {
    this.selector = selec;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object select(Object data) {
    return selector.select((T) data);
  }

  @Override
  public void prepare(Config cfg, TaskContext context) {

  }
}

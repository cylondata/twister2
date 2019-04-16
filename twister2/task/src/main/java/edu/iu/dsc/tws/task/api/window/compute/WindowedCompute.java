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
package edu.iu.dsc.tws.task.api.window.compute;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.api.typed.AbstractSingleDataCompute;
import edu.iu.dsc.tws.task.api.window.constant.Window;
import edu.iu.dsc.tws.task.api.window.policy.WindowingPolicy;

public abstract class WindowedCompute<T> extends AbstractSingleDataCompute<T> {

  private static final Logger LOG = Logger.getLogger(WindowedCompute.class.getName());

  private List<T> windowContent;

  private boolean status = false;

  private WindowingPolicy windowingPolicy;

  public abstract boolean window(List<T> content);

  public WindowedCompute(WindowingPolicy windowingPolicy) {
    this.windowingPolicy = windowingPolicy;
  }

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    super.prepare(cfg, ctx);
    this.windowContent = new ArrayList<>(this.windowingPolicy.getCount().value);
  }

  @Override
  public boolean execute(IMessage<T> content) {
    doWindowing(content);
    return status;
  }

  public void doWindowing(IMessage<T> content) {

    if (this.windowingPolicy.getWindow().equals(Window.TUMBLING)) {
      if (this.windowContent.size() < this.windowingPolicy.getCount().value) {
        this.windowContent.add(content.getContent());
      }

      if (this.windowContent.size() == this.windowingPolicy.getCount().value) {
        status = true;
        window(this.windowContent);
        this.windowContent.clear();
      } else {
        status = false;
      }
    }
  }
}

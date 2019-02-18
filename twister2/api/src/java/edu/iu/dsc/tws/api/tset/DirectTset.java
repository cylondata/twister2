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
package edu.iu.dsc.tws.api.tset;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;

public class DirectTset<T> extends BaseTSet<T> {
  private BaseTSet<T> parent;

  public DirectTset(Config cfg, TaskGraphBuilder bldr, BaseTSet<T> prnt) {
    super(cfg, bldr);
    this.parent = prnt;
    this.name = "direct-" + parent.getName();
  }

  @Override
  public boolean baseBuild() {
    return false;
  }

  @Override
  void buildConnection(ComputeConnection connection) {
    DataType dataType = getDataType(getType());

    connection.direct(parent.getName(), Constants.DEFAULT_EDGE, dataType);
  }
}

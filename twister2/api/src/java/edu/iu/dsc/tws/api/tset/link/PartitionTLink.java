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
package edu.iu.dsc.tws.api.tset.link;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.tset.Constants;
import edu.iu.dsc.tws.api.tset.PartitionFunction;
import edu.iu.dsc.tws.api.tset.TSetEnv;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.sets.BaseTSet;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.DataType;

public class PartitionTLink<T> extends edu.iu.dsc.tws.api.tset.link.BaseTLink<T> {
  private BaseTSet<T> parent;

  private PartitionFunction<T> partitionFunction;

  public PartitionTLink(Config cfg, TSetEnv tSetEnv, BaseTSet<T> prnt,
                        PartitionFunction<T> parFn) {
    super(cfg, tSetEnv);
    this.parent = prnt;
    this.partitionFunction = parFn;
    this.name = "partition-" + parent.getName();
  }

  @Override
  public String getName() {
    return parent.getName();
  }

  @Override
  public boolean baseBuild() {
    return true;
  }

  @Override
  public void buildConnection(ComputeConnection connection) {
    DataType dataType = TSetUtils.getDataType(getType());

    connection.partition(parent.getName(), Constants.DEFAULT_EDGE, dataType);
  }

  public PartitionFunction<T> getPartitionFunction() {
    return partitionFunction;
  }
}

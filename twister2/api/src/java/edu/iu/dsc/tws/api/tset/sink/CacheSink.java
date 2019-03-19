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
package edu.iu.dsc.tws.api.tset.sink;

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.tset.BaseSink;
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.dataset.impl.EntityPartition;

public class CacheSink<T> extends BaseSink<T> {
  private static final Logger LOG = Logger.getLogger(CacheSink.class.getName());

  private int count = 0;
  private DataObject<T> datapoints = null;

  public CacheSink(DataObject<T> datapoints) {
    this.datapoints = datapoints;
  }

  @Override
  public boolean add(T value) {
    // todo every time add is called, a new partition will be made! how to handle that?
    return addData(value);
  }

  @Override
  public void close() {

  }

  private boolean addData(T value) {
    int curr = datapoints.getPartitionCount();
    datapoints.addPartition(new EntityPartition<T>(curr, value)); //
    return false;
  }

  @Override
  public void prepare() {

  }
}

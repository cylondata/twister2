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
package edu.iu.dsc.tws.comms.dfw.io;

import java.util.ArrayList;
import java.util.List;

public class AggregatedObjects {
  private int source;

  private int destination;

  private List<Object> dataList = new ArrayList<>();

  private int flag = 0;

  public AggregatedObjects(int source, int destination) {
    this.source = source;
    this.destination = destination;
  }

  public boolean add(int dataFlag, Object data) {
    if (flag == dataFlag) {
      dataList.add(data);
      return true;
    }
    return false;
  }

  public int size() {
    return dataList.size();
  }

  public int getFlag() {
    return flag;
  }

  public Object getData(int index) {
    return dataList.get(index);
  }

  public int getSource() {
    return source;
  }

  public int getDestination() {
    return destination;
  }
}

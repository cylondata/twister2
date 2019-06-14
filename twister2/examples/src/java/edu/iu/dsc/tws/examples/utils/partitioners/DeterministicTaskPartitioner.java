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
package edu.iu.dsc.tws.examples.utils.partitioners;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import edu.iu.dsc.tws.task.api.TaskPartitioner;

public class DeterministicTaskPartitioner implements TaskPartitioner {

  private List<Integer> dst;

  @Override
  public void prepare(Set sources, Set destinations) {
    this.dst = new ArrayList<>(destinations);
    Collections.sort(this.dst);
  }

  @Override
  public int partition(int source, Object data) {
    return dst.get(source % dst.size());
  }

  @Override
  public void commit(int source, int partition) {

  }
}

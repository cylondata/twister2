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
package edu.iu.dsc.tws.api.tset.ops;

import java.util.List;

import edu.iu.dsc.tws.api.task.TaskContext;
import edu.iu.dsc.tws.api.task.graph.OperationMode;

public interface MultiOutEdgeOp {

  TaskContext getContext();

  List<String> getEdges();

  default <T> void writeToEdges(T output) {
    int i = 0;
    while (i < getEdges().size()) {
      getContext().write(getEdges().get(i), output);
      i++;
    }
  }

  default void writeEndToEdges() {
    if (getContext().getOperationMode() == OperationMode.STREAMING) {
      return;
    }

    int i = 0;
    while (i < getEdges().size()) {
      getContext().end(getEdges().get(i));
      i++;
    }
  }

  default <K, V> void keyedWriteToEdges(K key, V value) {
    int i = 0;
    while (i < getEdges().size()) {
      getContext().write(getEdges().get(i), key, value);
      i++;
    }
  }
}

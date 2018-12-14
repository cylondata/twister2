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

import java.util.LinkedList;
import java.util.List;

import edu.iu.dsc.tws.api.tset.Collector;
import edu.iu.dsc.tws.task.api.TaskContext;

public class CollectorImpl<T> implements Collector<T> {
  private boolean closed;

  private List<T> pendingRecords = new LinkedList<>();

  private TaskContext context;

  private String edge;

  public CollectorImpl(TaskContext ctx, String e) {
    this.context = ctx;
    this.edge = e;
  }

  @Override
  public void collect(T record) {
    while (pendingRecords.size() > 0) {
      T remove = pendingRecords.get(0);
      if (!context.write(edge, remove)) {
        pendingRecords.remove(0);
      } else {
        break;
      }
    }

    if (pendingRecords.size() == 0) {
      if (!context.write(edge, record)) {
        pendingRecords.add(record);
      }
    } else {
      pendingRecords.add(record);
    }
  }

  @Override
  public void close() {
    closed = true;
  }

  public boolean hasPending() {
    while (pendingRecords.size() > 0) {
      T remove = pendingRecords.get(0);
      if (!context.write(edge, remove)) {
        pendingRecords.remove(0);
      } else {
        break;
      }
    }
    return pendingRecords.size() > 0;
  }

  public boolean isClosed() {
    return closed;
  }
}

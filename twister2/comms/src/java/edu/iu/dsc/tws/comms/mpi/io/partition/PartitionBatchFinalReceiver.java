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
package edu.iu.dsc.tws.comms.mpi.io.partition;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.GatherBatchReceiver;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.mpi.MPIContext;
import edu.iu.dsc.tws.comms.mpi.MPIDataFlowPartition;
import edu.iu.dsc.tws.comms.shuffle.FSKeyedMerger;
import edu.iu.dsc.tws.comms.shuffle.FSKeyedSortedMerger;
import edu.iu.dsc.tws.comms.shuffle.FSMerger;

public class PartitionBatchFinalReceiver implements MessageReceiver {
  private static final Logger LOG = Logger.getLogger(PartitionBatchFinalReceiver.class.getName());
  private Map<Integer, Map<Integer, Boolean>> finished;

  private Map<Integer, Map<Integer, List<Object>>> data;

  private int messageCount = 0;

  private GatherBatchReceiver batchReceiver;

  private FSMerger merger;

  private FSKeyedSortedMerger sortedMerger;

  private FSKeyedMerger keyedMerger;

  private boolean sorted;

  private boolean disk;

  private Comparator<Object> comparator;

  private MPIDataFlowPartition partition;

  public PartitionBatchFinalReceiver(GatherBatchReceiver receiver, boolean srt, boolean d) {
    this.batchReceiver = receiver;
    this.sorted = srt;
    this.disk = d;
  }

  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    int maxBytesInMemory = MPIContext.getShuffleMaxBytesInMemory(cfg);
    int maxRecordsInMemory = MPIContext.getShuffleMaxRecordsInMemory(cfg);
    String path = MPIContext.getShuffleDirectoryPath(cfg);

    finished = new ConcurrentHashMap<>();
    for (Integer integer : expectedIds.keySet()) {
      Map<Integer, Boolean> perTarget = new ConcurrentHashMap<>();
      Map<Integer, List<Object>> d = new ConcurrentHashMap<>();
      for (Integer exp : expectedIds.get(integer)) {
        perTarget.put(exp, false);
        d.put(exp, new ArrayList<>());
      }
      finished.put(integer, perTarget);
    }

    partition = (MPIDataFlowPartition) op;
    if (partition.getKeyType() == null) {
      merger = new FSMerger(maxBytesInMemory, maxRecordsInMemory, path,
          getOperationName(), partition.getDataType());
    } else {
      if (sorted) {
        sortedMerger = new FSKeyedSortedMerger(maxBytesInMemory, maxRecordsInMemory, path,
            getOperationName(), partition.getKeyType(), partition.getDataType(), comparator);
      } else {
        keyedMerger = new FSKeyedMerger(maxBytesInMemory, maxRecordsInMemory, path,
            getOperationName(), partition.getKeyType(), partition.getDataType());
      }
    }
  }

  @Override
  public boolean onMessage(int source, int destination, int target, int flags, Object object) {
    // add the object to the map
    byte[] d = (byte[]) object;
    merger.add(d, d.length);
    return true;
  }

  @Override
  public void progress() {

  }

  private boolean isAllFinished(int target) {
    boolean isDone = true;
    for (Boolean bol : finished.get(target).values()) {
      isDone &= bol;
    }
    return isDone;
  }

  @Override
  public void onFinish() {
    merger.switchToReading();

    Iterator<Object> itr = merger.readIterator();
    batchReceiver.receive(0, itr);
  }

  private String getOperationName() {
    int edge = partition.getEdge();
    return "partition-" + edge;
  }
}

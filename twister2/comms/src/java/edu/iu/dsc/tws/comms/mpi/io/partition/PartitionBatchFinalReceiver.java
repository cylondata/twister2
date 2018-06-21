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

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.GatherBatchReceiver;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.mpi.MPIContext;
import edu.iu.dsc.tws.comms.mpi.MPIDataFlowPartition;
import edu.iu.dsc.tws.comms.mpi.io.KeyedContent;
import edu.iu.dsc.tws.comms.shuffle.FSKeyedMerger;
import edu.iu.dsc.tws.comms.shuffle.FSKeyedSortedMerger;
import edu.iu.dsc.tws.comms.shuffle.FSMerger;
import edu.iu.dsc.tws.comms.shuffle.Shuffle;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;

/**
 * A receiver that goes to disk
 */
public class PartitionBatchFinalReceiver implements MessageReceiver {
  private static final Logger LOG = Logger.getLogger(PartitionBatchFinalReceiver.class.getName());

  private Map<Integer, Map<Integer, Boolean>> finished;

  private GatherBatchReceiver batchReceiver;

  private Map<Integer, Shuffle> sortedMergers = new HashMap<>();

  private boolean sorted;

  private boolean disk;

  private Comparator<Object> comparator;

  private MPIDataFlowPartition partition;

  private boolean keyed;

  private KryoSerializer kryoSerializer;

  private int executor = 0;

  public PartitionBatchFinalReceiver(GatherBatchReceiver receiver, boolean srt,
                                     boolean d, Comparator<Object> com) {
    this.batchReceiver = receiver;
    this.sorted = srt;
    this.disk = d;
    this.kryoSerializer = new KryoSerializer();
    this.comparator = com;
  }

  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    int maxBytesInMemory = MPIContext.getShuffleMaxBytesInMemory(cfg);
    int maxRecordsInMemory = MPIContext.getShuffleMaxRecordsInMemory(cfg);
    String path = MPIContext.getShuffleDirectoryPath(cfg);

    executor = op.getTaskPlan().getThisExecutor();
    finished = new ConcurrentHashMap<>();
    partition = (MPIDataFlowPartition) op;
    keyed = partition.getKeyType() != null;
    for (Integer target : expectedIds.keySet()) {
      Map<Integer, Boolean> perTarget = new ConcurrentHashMap<>();
      for (Integer exp : expectedIds.get(target)) {
        perTarget.put(exp, false);
      }
      finished.put(target, perTarget);

      Shuffle sortedMerger;
      if (partition.getKeyType() == null) {
        sortedMerger = new FSMerger(maxBytesInMemory, maxRecordsInMemory, path,
            getOperationName(target), partition.getDataType());
      } else {
        if (sorted) {
          sortedMerger = new FSKeyedSortedMerger(maxBytesInMemory, maxRecordsInMemory, path,
              getOperationName(target), partition.getKeyType(),
              partition.getDataType(), comparator, target);
        } else {
          sortedMerger = new FSKeyedMerger(maxBytesInMemory, maxRecordsInMemory, path,
              getOperationName(target), partition.getKeyType(), partition.getDataType());
        }
      }
      sortedMergers.put(target, sortedMerger);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean onMessage(int source, int destination, int target, int flags, Object object) {
    Shuffle sortedMerger = sortedMergers.get(target);
    if (sortedMerger == null) {
      throw new RuntimeException("Un-expected target: " + target);
    }
    LOG.info(String.format("Receive message %d", target));
    // add the object to the map
    if (keyed) {
      List<KeyedContent> keyedContents = (List<KeyedContent>) object;
      for (KeyedContent kc : keyedContents) {
        Object data = kc.getValue();
        byte[] d = kryoSerializer.serialize(data);

        sortedMerger.add(kc.getKey(), d, d.length);
      }
    } else {
      List<Object> contents = (List<Object>) object;
      for (Object kc : contents) {
        byte[] d = kryoSerializer.serialize(kc);
        sortedMerger.add(d, d.length);
      }
    }
    return true;
  }

  @Override
  public void progress() {
    for (Shuffle sorts : sortedMergers.values()) {
      sorts.run();
    }
  }

  @Override
  public void onFinish(int target) {
    Shuffle sortedMerger = sortedMergers.get(target);
    sortedMerger.switchToReading();
    Iterator<Object> itr = sortedMerger.readIterator();
    try {
      batchReceiver.receive(target, itr);
    } catch (RuntimeException e) {
      LOG.log(Level.INFO, "Array index: exe " + executor + " target: " + target, e);
    }
  }

  private String getOperationName(int target) {
    int edge = partition.getEdge();
    return "partition-" + edge + "-" + target;
  }
}

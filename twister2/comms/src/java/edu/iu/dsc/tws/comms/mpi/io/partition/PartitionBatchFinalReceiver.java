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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.GatherBatchReceiver;
import edu.iu.dsc.tws.comms.api.MessageFlags;
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

  private GatherBatchReceiver batchReceiver;

  private Map<Integer, Shuffle> sortedMergers = new HashMap<>();

  private boolean sorted;

  private boolean disk;

  private Comparator<Object> comparator;

  private MPIDataFlowPartition partition;

  private boolean keyed;

  private KryoSerializer kryoSerializer;

  private int executor = 0;

  private Map<Integer, Integer> totalReceives = new HashMap<>();

  /**
   * Finished sources per target (target -> finished sources)
   */
  private Map<Integer, Set<Integer>> finishedSources = new HashMap<>();

  /**
   * After all the sources finished for a target we add to this set
   */
  private Set<Integer> finishedTargets = new HashSet<>();

  /**
   * We add to this set after calling receive
   */
  private Set<Integer> finishedTargetsCompleted = new HashSet<>();

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
    finishedSources = new HashMap<>();
    partition = (MPIDataFlowPartition) op;
    keyed = partition.getKeyType() != null;
    for (Integer target : expectedIds.keySet()) {
      Map<Integer, Boolean> perTarget = new ConcurrentHashMap<>();
      for (Integer exp : expectedIds.get(target)) {
        perTarget.put(exp, false);
      }

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
      totalReceives.put(target, 0);
      finishedSources.put(target, new HashSet<>());
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

    if ((flags & MessageFlags.EMPTY) == MessageFlags.EMPTY) {
      Set<Integer> finished = finishedSources.get(target);
      if (finished.contains(source)) {
        LOG.log(Level.WARNING, String.format("Duplicate finish from source %d", source));
      } else {
        finished.add(source);
      }
      if (finished.size() == partition.getSources().size()) {
        finishedTargets.add(target);
      }
      return true;
    }

    // add the object to the map
    if (keyed) {
      List<KeyedContent> keyedContents = (List<KeyedContent>) object;
      for (KeyedContent kc : keyedContents) {
        Object data = kc.getValue();
        byte[] d = kryoSerializer.serialize(data);

        sortedMerger.add(kc.getKey(), d, d.length);
      }
      int total = totalReceives.get(target);
      total += keyedContents.size();
      totalReceives.put(target, total);
    } else {
      List<Object> contents = (List<Object>) object;
      for (Object kc : contents) {
        byte[] d = kryoSerializer.serialize(kc);
        sortedMerger.add(d, d.length);
      }
      int total = totalReceives.get(target);
      total += contents.size();
      totalReceives.put(target, total);
    }
    LOG.info(String.format("%d %d On Message totals %s", executor, target, totalReceives));
    return true;
  }

  @Override
  public void progress() {
    for (Shuffle sorts : sortedMergers.values()) {
      sorts.run();
    }

    for (int i : finishedTargets) {
      if (!finishedTargetsCompleted.contains(i)) {
        onFinish(i);
        finishedTargetsCompleted.add(i);
      }
    }
  }

  @Override
  public void onFinish(int target) {
    Shuffle sortedMerger = sortedMergers.get(target);
    sortedMerger.switchToReading();
    Iterator<Object> itr = sortedMerger.readIterator();
    LOG.info(String.format("%d %d On finish totals %s", executor, target, totalReceives));
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

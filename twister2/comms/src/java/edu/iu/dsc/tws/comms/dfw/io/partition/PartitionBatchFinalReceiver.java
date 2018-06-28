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
package edu.iu.dsc.tws.comms.dfw.io.partition;

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
import edu.iu.dsc.tws.comms.api.BatchReceiver;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.dfw.DataFlowContext;
import edu.iu.dsc.tws.comms.dfw.DataFlowPartition;
import edu.iu.dsc.tws.comms.dfw.io.KeyedContent;
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

  /**
   * The receiver
   */
  private BatchReceiver batchReceiver;

  /**
   * Sort mergers for each target
   */
  private Map<Integer, Shuffle> sortedMergers = new HashMap<>();

  /**
   * weather we need to sort the records according to key
   */
  private boolean sorted;

  private boolean disk;

  /**
   * Comparator for sorting records
   */
  private Comparator<Object> comparator;

  /**
   * The operation
   */
  private DataFlowPartition partition;

  /**
   * Weather a keyed operation is used
   */
  private boolean keyed;

  private KryoSerializer kryoSerializer;

  /**
   * The worker id
   */
  private int worker = 0;

  /**
   * Keep track of totals for debug purposes
   */
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

  public PartitionBatchFinalReceiver(BatchReceiver receiver, boolean srt,
                                     boolean d, Comparator<Object> com) {
    this.batchReceiver = receiver;
    this.sorted = srt;
    this.disk = d;
    this.kryoSerializer = new KryoSerializer();
    this.comparator = com;
  }

  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    int maxBytesInMemory = DataFlowContext.getShuffleMaxBytesInMemory(cfg);
    int maxRecordsInMemory = DataFlowContext.getShuffleMaxRecordsInMemory(cfg);
    String path = DataFlowContext.getShuffleDirectoryPath(cfg);

    worker = op.getTaskPlan().getThisExecutor();
    finishedSources = new HashMap<>();
    partition = (DataFlowPartition) op;
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
      throw new RuntimeException("Un-expected target id: " + target);
    }

    if ((flags & MessageFlags.EMPTY) == MessageFlags.EMPTY) {
      Set<Integer> finished = finishedSources.get(target);
      if (finished.contains(source)) {
        LOG.log(Level.WARNING,
            String.format("%d Duplicate finish from source id %d", worker, source));
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
    batchReceiver.receive(target, itr);
  }

  private String getOperationName(int target) {
    int edge = partition.getEdge();
    return "partition-" + edge + "-" + target;
  }
}

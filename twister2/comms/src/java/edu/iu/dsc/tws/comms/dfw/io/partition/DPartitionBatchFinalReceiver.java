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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.dfw.DataFlowContext;
import edu.iu.dsc.tws.comms.dfw.DataFlowPartition;
import edu.iu.dsc.tws.comms.dfw.io.KeyedContent;
import edu.iu.dsc.tws.comms.dfw.io.types.DataSerializer;
import edu.iu.dsc.tws.comms.shuffle.FSKeyedMerger;
import edu.iu.dsc.tws.comms.shuffle.FSKeyedSortedMerger;
import edu.iu.dsc.tws.comms.shuffle.FSMerger;
import edu.iu.dsc.tws.comms.shuffle.Shuffle;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;

/**
 * A receiver that goes to disk
 */
public class DPartitionBatchFinalReceiver implements MessageReceiver {
  private static final Logger LOG = Logger.getLogger(DPartitionBatchFinalReceiver.class.getName());

  /**
   * The receiver
   */
  private BulkReceiver bulkReceiver;

  /**
   * Sort mergers for each target
   */
  private Map<Integer, Shuffle> sortedMergers = new HashMap<>();

  /**
   * weather we need to sort the records according to key
   */
  private boolean sorted;

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
  private int thisWorker = 0;

  /**
   * Keep track of totals for debug purposes
   */
  private Map<Integer, Integer> totalReceives = new HashMap<>();

  /**
   * Finished workers per target (target -> finished workers)
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

  /**
   * Weather everyone finished
   */
  private Set<Integer> targets = new HashSet<>();

  /**
   * The directory in which we will be saving the shuffle objects
   */
  private String shuffleDirectory;

  public DPartitionBatchFinalReceiver(BulkReceiver receiver, boolean srt,
                                      String shuffleDir, Comparator<Object> com) {
    this.bulkReceiver = receiver;
    this.sorted = srt;
    this.kryoSerializer = new KryoSerializer();
    this.comparator = com;
    this.shuffleDirectory = shuffleDir;
  }

  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    int maxBytesInMemory = DataFlowContext.getShuffleMaxBytesInMemory(cfg);
    int maxRecordsInMemory = DataFlowContext.getShuffleMaxRecordsInMemory(cfg);

    thisWorker = op.getTaskPlan().getThisExecutor();
    finishedSources = new HashMap<>();
    partition = (DataFlowPartition) op;
    keyed = partition.getKeyType() != null;
    targets = new HashSet<>(expectedIds.keySet());

    for (Integer target : expectedIds.keySet()) {
      Map<Integer, Boolean> perTarget = new ConcurrentHashMap<>();
      for (Integer exp : expectedIds.get(target)) {
        perTarget.put(exp, false);
      }

      Shuffle sortedMerger;
      if (partition.getKeyType() == null) {
        sortedMerger = new FSMerger(maxBytesInMemory, maxRecordsInMemory, shuffleDirectory,
            getOperationName(target), partition.getDataType());
      } else {
        if (sorted) {
          sortedMerger = new FSKeyedSortedMerger(maxBytesInMemory, maxRecordsInMemory,
              shuffleDirectory, getOperationName(target), partition.getKeyType(),
              partition.getDataType(), comparator, target);
        } else {
          sortedMerger = new FSKeyedMerger(maxBytesInMemory, maxRecordsInMemory, shuffleDirectory,
              getOperationName(target), partition.getKeyType(), partition.getDataType());
        }
      }
      sortedMergers.put(target, sortedMerger);
      totalReceives.put(target, 0);
      finishedSources.put(target, new HashSet<>());
    }
    this.bulkReceiver.init(cfg, expectedIds.keySet());
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    Shuffle sortedMerger = sortedMergers.get(target);
    if (sortedMerger == null) {
      throw new RuntimeException("Un-expected target id: " + target);
    }

    if ((flags & MessageFlags.END) == MessageFlags.END) {
      Set<Integer> finished = finishedSources.get(target);
      if (finished.contains(source)) {
        LOG.log(Level.WARNING,
            String.format("%d Duplicate finish from source id %d", this.thisWorker, source));
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
        byte[] d = DataSerializer.serialize(data, kryoSerializer);

        sortedMerger.add(kc.getKey(), d, d.length);
      }
      int total = totalReceives.get(target);
      total += keyedContents.size();
      totalReceives.put(target, total);
    } else {
      List<Object> contents = (List<Object>) object;
      for (Object kc : contents) {
        byte[] d = DataSerializer.serialize(kc, kryoSerializer);
        sortedMerger.add(d, d.length);
      }
      int total = totalReceives.get(target);
      total += contents.size();
      totalReceives.put(target, total);
    }
    return true;
  }

  @Override
  public boolean progress() {
    for (Shuffle sorts : sortedMergers.values()) {
      sorts.run();
    }

    for (int i : finishedTargets) {
      if (!finishedTargetsCompleted.contains(i)) {
        finishTarget(i);
        finishedTargetsCompleted.add(i);
      }
    }

    return !finishedTargets.equals(targets);
  }

  private void finishTarget(int target) {
    Shuffle sortedMerger = sortedMergers.get(target);
    sortedMerger.switchToReading();
    Iterator<Object> itr = sortedMerger.readIterator();
    bulkReceiver.receive(target, itr);
    onFinish(target);
  }

  @Override
  public void onFinish(int source) {
  }

  private String getOperationName(int target) {
    int edge = partition.getEdge();
    return "partition-" + edge + "-" + target + "-" + UUID.randomUUID().toString();
  }
}

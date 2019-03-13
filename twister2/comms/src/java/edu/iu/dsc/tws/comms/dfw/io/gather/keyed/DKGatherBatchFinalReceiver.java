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
package edu.iu.dsc.tws.comms.dfw.io.gather.keyed;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.dfw.DataFlowContext;
import edu.iu.dsc.tws.comms.dfw.io.KeyedReceiver;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.comms.dfw.io.types.DataSerializer;
import edu.iu.dsc.tws.comms.shuffle.FSKeyedMerger;
import edu.iu.dsc.tws.comms.shuffle.FSKeyedSortedMerger;
import edu.iu.dsc.tws.comms.shuffle.Shuffle;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;

/**
 * Disk based Final receiver for keyed gather
 */
public class DKGatherBatchFinalReceiver extends KeyedReceiver {

  /**
   * Final receiver that get the reduced values for the operation
   */
  private BulkReceiver bulkReceiver;

  /**
   * Sort mergers for each target
   */
  private Map<Integer, Shuffle> sortedMergers = new HashMap<>();


  /**
   * Comparator for sorting records
   */
  private Comparator<Object> comparator;

  /**
   * Serializer used to convert between objects and byte streams
   */
  private KryoSerializer kryoSerializer;

  /**
   * Shuffler directory
   */
  private String shuffleDirectory;

  /**
   * weather we need to sort the records according to key
   */
  private boolean sorted;


  public DKGatherBatchFinalReceiver(BulkReceiver receiver, boolean srt, int limitPerKey,
                                    String shuffleDir, Comparator<Object> com) {
    this.bulkReceiver = receiver;
    this.sorted = srt;
    this.limitPerKey = limitPerKey;
    //Setting this to false since we want keep the data kept in memory limited, since this is the
    //disk based implementation
    this.isFinalBatchReceiver = false;
    this.shuffleDirectory = shuffleDir;
    this.comparator = com;
    this.kryoSerializer = new KryoSerializer();
  }

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    super.init(cfg, op, expectedIds);
    int maxBytesInMemory = DataFlowContext.getShuffleMaxBytesInMemory(cfg);
    int maxRecordsInMemory = DataFlowContext.getShuffleMaxRecordsInMemory(cfg);

    for (Integer target : expectedIds.keySet()) {

      Shuffle sortedMerger;
      if (sorted) {
        sortedMerger = new FSKeyedSortedMerger(maxBytesInMemory, maxRecordsInMemory,
            shuffleDirectory, getOperationName(target), dataFlowOperation.getKeyType(),
            dataFlowOperation.getDataType(), comparator, target);
      } else {
        sortedMerger = new FSKeyedMerger(maxBytesInMemory, maxRecordsInMemory, shuffleDirectory,
            getOperationName(target), dataFlowOperation.getKeyType(),
            dataFlowOperation.getDataType());
      }
      sortedMergers.put(target, sortedMerger);
    }
    this.bulkReceiver.init(cfg, expectedIds.keySet());

  }

  private String getOperationName(int target) {
    String uid = dataFlowOperation.getUniqueId();
    return "gather-" + uid + "-" + target + "-" + UUID.randomUUID().toString();
  }

  /**
   * Called from the progress method to perform the communication calls to send the queued messages
   * Since this is the disk based gather this method will save the values to disk
   *
   * @param needsFurtherProgress current state of needsFurtherProgress value
   * @param sourcesFinished specifies if the sources have completed
   * @param target the target(which is a source in this instance) from which the messages are sent
   * @param targetSendQueue the data structure that contains all the message data
   * @return true if further progress is needed or false otherwise
   */
  @Override
  protected boolean sendToTarget(boolean needsFurtherProgress, boolean sourcesFinished, int target,
                                 Queue<Object> targetSendQueue) {
    Shuffle sortedMerger = sortedMergers.get(target);
    while (!targetSendQueue.isEmpty()) {
      Tuple kc = (Tuple) targetSendQueue.poll();
      Object data = kc.getValue();
      byte[] d = DataSerializer.serialize(data, kryoSerializer);
      sortedMerger.add(kc.getKey(), d, d.length);
    }

    //write to disk if overflows
    sortedMerger.run();

    return needsFurtherProgress;
  }

  /**
   * Performs the final steps of the progress method in the receiver. Since this is the disk based
   * gather the finish method will signal the Shuffle that the provided target is completed.
   *
   * @param needsFurtherProgress current state of needsFurtherProgress value
   * @param target the target(which is a source in this instance) from which the messages are sent
   * @return true if further progress is needed or false otherwise
   */
  protected boolean finishProgress(boolean needsFurtherProgress, int target) {

    batchDone.put(target, true);
    Shuffle sortedMerger = sortedMergers.get(target);
    sortedMerger.switchToReading();
    Iterator<Object> itr = sortedMerger.readIterator();
    bulkReceiver.receive(target, itr);

    return needsFurtherProgress;
  }

  /**
   * checks if the Empty message was sent for this target and sends it if not sent and possible to
   * send. Since this is the final receiver we do not need any functionality to send empty message
   * so the method is overwritten to simply return true
   *
   * @param target target for which the check is done
   * @return false if Empty is sent
   */
  @Override
  protected boolean checkIfEmptyIsSent(int target) {
    return true;
  }
}

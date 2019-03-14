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

package org.apache.storm.spout;

import java.util.Map;
import java.util.logging.Logger;

import org.apache.storm.Config;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.state.StateFactory;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import static org.apache.storm.spout.CheckPointState.Action;
import static org.apache.storm.spout.CheckPointState.State.COMMITTED;

/**
 * Emits checkpoint tuples which is used to save the state of the {@link org.apache.storm.topology.IStatefulComponent} across the topology.
 * If a topology contains Stateful bolts, Checkpoint spouts are automatically added to the topology.
 * There is only one Checkpoint task per
 * topology. Checkpoint spout stores its internal state in a {@link KeyValueState}.
 *
 * @see CheckPointState
 */
public class CheckpointSpout extends BaseRichSpout {

  public static final String CHECKPOINT_STREAM_ID = "$checkpoint";
  public static final String CHECKPOINT_COMPONENT_ID = "$checkpointspout";
  public static final String CHECKPOINT_FIELD_TXID = "txid";
  public static final String CHECKPOINT_FIELD_ACTION = "action";
  private static final Logger LOG = Logger.getLogger(CheckpointSpout.class.getName());
  private static final String TX_STATE_KEY = "__state";
  private TopologyContext context;
  private SpoutOutputCollector collector;
  private long lastCheckpointTs;
  private int checkpointInterval;
  private int sleepInterval;
  private boolean recoveryStepInProgress;
  private boolean checkpointStepInProgress;
  private boolean recovering;
  private KeyValueState<String, CheckPointState> checkpointState;
  private CheckPointState curTxState;

  public static boolean isCheckpoint(Tuple input) {
    return CHECKPOINT_STREAM_ID.equals(input.getSourceStreamId());
  }

  @Override
  public void open(Map<String, Object> conf, TopologyContext ctx,
                   SpoutOutputCollector cllctr) {
    open(ctx, cllctr, loadCheckpointInterval(conf), loadCheckpointState(conf, ctx));
  }

  // package access for unit test
  void open(TopologyContext ctx, SpoutOutputCollector collctr,
            int checkPInterval, KeyValueState<String, CheckPointState> checkPState) {
    this.context = ctx;
    this.collector = collctr;
    this.checkpointInterval = checkPInterval;
    this.sleepInterval = checkPInterval / 10;
    this.checkpointState = checkPState;
    this.curTxState = checkPState.get(TX_STATE_KEY);
    lastCheckpointTs = 0;
    recoveryStepInProgress = false;
    checkpointStepInProgress = false;
    recovering = true;
  }

  @Override
  public void nextTuple() {
    if (shouldRecover()) {
      handleRecovery();
      startProgress();
    } else if (shouldCheckpoint()) {
      doCheckpoint();
      startProgress();
    } else {
      Utils.sleep(sleepInterval);
    }
  }

  @Override
  public void ack(Object msgId) {
    LOG.fine(() -> String.format("Got ack with txid %s, current txState %s", msgId, curTxState));
    if (curTxState.getTxid() == ((Number) msgId).longValue()) {
      if (recovering) {
        handleRecoveryAck();
      } else {
        handleCheckpointAck();
      }
    } else {
      LOG.warning(() -> String.format(
          "Ack msgid %s, txState.txid %d mismatch", msgId, curTxState.getTxid()));
    }
    resetProgress();
  }

  @Override
  public void fail(Object msgId) {
    LOG.fine(() -> String.format("Got fail with msgid %s", msgId));
    if (!recovering) {
      LOG.fine("Checkpoint failed, will trigger recovery");
      recovering = true;
    }
    resetProgress();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream(CHECKPOINT_STREAM_ID,
        new Fields(CHECKPOINT_FIELD_TXID, CHECKPOINT_FIELD_ACTION));
  }

  /**
   * Loads the last saved checkpoint state the from persistent storage.
   */
  private KeyValueState<String, CheckPointState> loadCheckpointState(Map<String, Object> conf,
                                                                     TopologyContext ctx) {
    String namespace = ctx.getThisComponentId() + "-" + ctx.getThisTaskId();
    KeyValueState<String, CheckPointState> state
        = (KeyValueState<String, CheckPointState>) StateFactory.getState(namespace, conf, ctx);
    if (state.get(TX_STATE_KEY) == null) {
      CheckPointState txState = new CheckPointState(-1, COMMITTED);
      state.put(TX_STATE_KEY, txState);
      state.commit();
      LOG.fine(() -> String.format("Initialized checkpoint spout state with txState %s", txState));
    } else {
      LOG.fine(() -> String.format("Got checkpoint spout state %s", state.get(TX_STATE_KEY)));
    }
    return state;
  }

  private int loadCheckpointInterval(Map<String, Object> topoConf) {
    int interval = 0;
    if (topoConf.containsKey(Config.TOPOLOGY_STATE_CHECKPOINT_INTERVAL)) {
      interval = ((Number) topoConf.get(Config.TOPOLOGY_STATE_CHECKPOINT_INTERVAL)).intValue();
    }
    // ensure checkpoint interval is not less than a sane low value.
    interval = Math.max(100, interval);
    LOG.info(String.format("Checkpoint interval is %d millis", interval));
    return interval;
  }

  private boolean shouldRecover() {
    return recovering && !recoveryStepInProgress;
  }

  private boolean shouldCheckpoint() {
    return !recovering && !checkpointStepInProgress
        && (curTxState.getState() != COMMITTED || checkpointIntervalElapsed());
  }

  private boolean checkpointIntervalElapsed() {
    return (System.currentTimeMillis() - lastCheckpointTs) > checkpointInterval;
  }

  private void handleRecovery() {
    LOG.fine("In recovery");
    Action action = curTxState.nextAction(true);
    emit(curTxState.getTxid(), action);
  }

  private void handleRecoveryAck() {
    CheckPointState nextState = curTxState.nextState(true);
    if (curTxState != nextState) {
      saveTxState(nextState);
    } else {
      LOG.fine(() -> String.format("Recovery complete, current state %s", curTxState));
      recovering = false;
    }
  }

  private void doCheckpoint() {
    LOG.fine("In checkpoint");
    if (curTxState.getState() == COMMITTED) {
      saveTxState(curTxState.nextState(false));
      lastCheckpointTs = System.currentTimeMillis();
    }
    Action action = curTxState.nextAction(false);
    emit(curTxState.getTxid(), action);
  }

  private void handleCheckpointAck() {
    CheckPointState nextState = curTxState.nextState(false);
    saveTxState(nextState);
  }

  private void emit(long txid, Action action) {
    LOG.fine(() -> String.format("Current state %s, emitting txid %d, action %s",
        curTxState, txid, action));
    collector.emit(CHECKPOINT_STREAM_ID, new Values(txid, action), txid);
  }

  private void saveTxState(CheckPointState txState) {
    LOG.fine(() -> String.format("saveTxState, current state %s -> new state %s",
        curTxState, txState));
    checkpointState.put(TX_STATE_KEY, txState);
    checkpointState.commit();
    curTxState = txState;
  }

  private void startProgress() {
    if (recovering) {
      recoveryStepInProgress = true;
    } else {
      checkpointStepInProgress = true;
    }
  }

  private void resetProgress() {
    if (recovering) {
      recoveryStepInProgress = false;
    } else {
      checkpointStepInProgress = false;
    }
  }
}

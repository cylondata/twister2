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
package org.apache.storm.topology.twister2;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Twister2Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.api.ICompute;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.ISink;
import edu.iu.dsc.tws.task.api.TaskContext;

public class Twister2Bolt implements ICompute, ISink, Twister2StormNode {

  private static final Logger LOG = Logger.getLogger(Twister2Bolt.class.getName());

  private IRichBolt stormBolt;
  private IBasicBolt stormBasicBolt;

  private Twister2BoltDeclarer boltDeclarer;
  private Integer parallelism = 1;
  private String id;

  private OutputCollector outputCollector;
  private BasicOutputCollector basicOutputCollector;

  private HashMap<String, Fields> inboundEdgeToFieldsMap = new HashMap<>();

  private final EdgeFieldMap outFieldsForEdge;
  private final EdgeFieldMap keyedOutEdges;

  private Twister2Bolt(String id, MadeASourceListener madeASourceListener) {
    this.id = id;
    this.boltDeclarer = new Twister2BoltDeclarer(madeASourceListener);
    this.outFieldsForEdge = new EdgeFieldMap(Utils.getDefaultStream(id));
    this.keyedOutEdges = new EdgeFieldMap(Utils.getDefaultStream(id));
  }

  public Twister2Bolt(String id, IRichBolt stormBolt, MadeASourceListener madeASourceListener) {
    this(id, madeASourceListener);
    this.stormBolt = stormBolt;
    this.stormBolt.declareOutputFields(this.outFieldsForEdge);
  }

  public Twister2Bolt(String id,
                      IBasicBolt stormBasicBolt,
                      MadeASourceListener madeASourceListener) {
    this(id, madeASourceListener);
    this.stormBasicBolt = stormBasicBolt;
    this.stormBasicBolt.declareOutputFields(this.outFieldsForEdge);
  }

  public Integer getParallelism() {
    return parallelism;
  }

  public void setParallelism(Integer parallelism) {
    this.parallelism = parallelism;
  }

  public Twister2BoltDeclarer getBoltDeclarer() {
    return boltDeclarer;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public Fields getOutFieldsForEdge(String edge) {
    return outFieldsForEdge.get(edge);
  }

  @Override
  public void setKeyedOutEdges(String edge, Fields keys) {
    LOG.info(String.format("[Storm-Bolt : %s] Setting out edge %s "
        + "with key fields %s", id, edge, keys));
    this.keyedOutEdges.put(edge, keys);
  }

  public void addInboundFieldsForEdge(String edge, Fields fields) {
    LOG.info(String.format("[Storm-Bolt : %s] Adding inbound fields for edge %s "
        + "with fields %s", id, edge, fields));
    this.inboundEdgeToFieldsMap.put(edge, fields);
  }

  private void createAndFireTuple(Object values, IMessage iMessage) {
    if (values instanceof Values) {
      Twister2Tuple twister2Tuple = new Twister2Tuple(
          this.inboundEdgeToFieldsMap.get(iMessage.edge()),
          (Values) values,
          iMessage
      );
      if (this.stormBolt != null) {
        this.stormBolt.execute(twister2Tuple);
      } else {
        this.stormBasicBolt.execute(twister2Tuple, this.basicOutputCollector);
      }
    } else {
      throw new RuntimeException("Unexpected message format. "
          + "Expected " + Values.class + " found " + values.getClass());
    }
  }

  @Override
  public boolean execute(IMessage message) {
    LOG.finest("Message received from edge " + message.edge() + " to " + this.id);
    //System.out.println("IMessage received from edge " + content.edge() + " to " + this.id);
    //todo handle below ArrayList<tws.Tuple> internally in core
    Object messageContent = message.getContent();
    if (messageContent instanceof Iterator) {
      Iterator valuesIterator = (Iterator) messageContent;
      while (valuesIterator.hasNext()) {
        this.createAndFireTuple(
            valuesIterator.next(),
            message
        );
      }
    } else if (messageContent instanceof List) {
      List valuesList = (List) messageContent;
      for (Object values : valuesList) {
        if (values instanceof edu.iu.dsc.tws.comms.dfw.io.Tuple) {
          this.createAndFireTuple(
              ((edu.iu.dsc.tws.comms.dfw.io.Tuple) values).getValue(),
              message
          );
        } else {
          this.createAndFireTuple(
              values,
              message
          );
        }
      }
    } else {
      throw new RuntimeException("Unexpected message content format.");
    }
    return false;
  }

  @Override
  public void prepare(Config cfg, TaskContext context) {
    LOG.info("Preparing storm-bolt : " + this.id);
    this.outputCollector = new OutputCollector(new IOutputCollector() {
      @Override
      public List<Integer> emit(String streamId,
                                Collection<Tuple> anchors,
                                List<Object> tuple) {
        if (!keyedOutEdges.containsKey(streamId)) { //not keyed
          context.write(streamId, tuple);
        } else { //generate the key and write
          Fields allFields = outFieldsForEdge.get(streamId);
          Fields fieldsForKey = keyedOutEdges.get(streamId);
          List<Object> key = allFields.select(fieldsForKey, tuple);
          context.write(streamId, key, tuple);
        }
        return Collections.singletonList(0);
      }

      @Override
      public void emitDirect(int taskId,
                             String streamId,
                             Collection<Tuple> anchors,
                             List<Object> tuple) {
        //todo
        throw new UnsupportedOperationException("Emit direct is not supported yet");
      }

      @Override
      public void ack(Tuple input) {

      }

      @Override
      public void fail(Tuple input) {

      }

      @Override
      public void reportError(Throwable error) {
        LOG.warning("Error occurred when emitting : " + error.getMessage());
      }
    });

    TopologyContext topologyContext = new TopologyContext(context);

    if (stormBolt != null) {
      this.stormBolt.prepare(
          cfg.toMap(),
          topologyContext,
          this.outputCollector
      );
    } else {
      this.basicOutputCollector = new BasicOutputCollector(
          this.outputCollector,
          Utils.getDefaultStream(this.id)
      );
      this.stormBasicBolt.prepare(
          cfg.toMap(),
          topologyContext
      );
    }
  }
}

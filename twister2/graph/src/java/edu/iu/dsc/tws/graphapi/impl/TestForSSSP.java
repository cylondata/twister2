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
package edu.iu.dsc.tws.graphapi.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.graphapi.api.BasicComputation;
import edu.iu.dsc.tws.graphapi.api.VertexStatusForInt;
import edu.iu.dsc.tws.graphapi.vertex.DefaultEdge;
import edu.iu.dsc.tws.graphapi.vertex.DefaultVertex;
import edu.iu.dsc.tws.graphapi.vertex.VertexStatus;

public class TestForSSSP extends BasicComputation implements Serializable {

  private GraphPartiton graphPartiton = new DataPartionSSSP(Context.TWISTER2_DIRECT_EDGE,
      parallelism, graphsize);
  private GraphInitialization graphInitialization = new DataIniSSSP(Context.TWISTER2_DIRECT_EDGE,
      parallelism, graphsize);

  private SourceTask sourceTask = new Task1();
  private ComputeTask computeTask = new Task2();
  private ReductionFunction reductionFunction = new RedFun();

  @Override
  public ComputeGraph computation() {
    return buildComputationTG(parallelism, config, sourceTask, computeTask,
        reductionFunction, MessageTypes.INTEGER_ARRAY);
  }

  @Override
  public ComputeGraph graphInitialization() {
    return buildGraphInitialaizationTG(dataDirectory, graphsize, parallelism, config,
        graphInitialization);
  }

  @Override
  public ComputeGraph graphpartition() {
    return buildGraphPartitionTG(dataDirectory, graphsize, parallelism, config,
        graphPartiton);
  }


  private class DataPartionSSSP extends GraphPartiton {

    private HashMap<String, DefaultVertex> hashMap;

    DataPartionSSSP(String edgename, int dsize, int parallel) {
      super(edgename, dsize, parallel);
      hashMap = new HashMap<String, DefaultVertex>();
    }

    @Override
    public void constructDataStr(IMessage message) {
      while (((Iterator) message.getContent()).hasNext()) {

        DefaultVertex defaultVertex = new DefaultVertex();
        ArrayList<DefaultEdge> arrayList = new ArrayList<>();
        String val = String.valueOf(((Iterator) message.getContent()).next());
        String[] data = val.split("\\s+");

        defaultVertex.initialize(data[0], 0, arrayList);

        ArrayList<String> adjList = new ArrayList<String>(Arrays.asList(data));
        adjList.remove(0);
        for (int i = 0; i < (adjList.size()) / 2; i++) {
          DefaultEdge defaultEdge = new DefaultEdge();
          String adjVertex = adjList.get(2 * i);
          int edgeValue = Integer.parseInt(adjList.get(2 * i + 1));
          defaultEdge.setId(adjVertex);
          defaultEdge.setValue(edgeValue);
          defaultVertex.addEdge(defaultEdge);
        }
        if (sourceVertexGlobal.equals(data[0])) {
          defaultVertex.setValue(0);
        } else {
          defaultVertex.setValue(Integer.MAX_VALUE);
        }
        hashMap.put(data[0], defaultVertex);
        writeMsgOnEdge(getEdgeName(), hashMap);
      }

    }
  }

  private class DataIniSSSP extends GraphInitialization {


    DataIniSSSP(String edgename, int dsize, int parallel) {
      super(edgename, dsize, parallel);
    }

    @Override
    public void constructDataStr(HashMap<String, VertexStatus> hashMap, IMessage message) {
      while (((Iterator) message.getContent()).hasNext()) {
        String val = String.valueOf(((Iterator) message.getContent()).next());
        String[] data = val.split("\\s+");
        VertexStatusForInt vertexStatusForInt = new VertexStatusForInt();
        if (data.length == 1 && data[0].equals("")) {
          continue;
        } else if (!data[0].equals("")) {
          if (sourceVertexGlobal.equals(data[0])) {
            vertexStatusForInt.setId(data[0]);
            vertexStatusForInt.setValue(0);
          } else {
            vertexStatusForInt.setId(data[0]);
            vertexStatusForInt.setValue(Integer.MAX_VALUE);
          }
          hashMap.put(data[0], vertexStatusForInt);
          writeMsgOnEdge(getEdgeName(), hashMap);
        }
      }
    }
  }

  private class Task1 extends SourceTask {

    @Override
    public void sendmessage(HashMap<String, DefaultVertex> graphData,
                            HashMap<String, VertexStatus> graphSsspStatus) {
      if (graphSsspStatus.size() != 0) {

        if (count < graphData.size()) {
          for (int i = 0; i < graphData.size(); i++) {
            String key = (String) graphData.keySet().toArray()[i];
            DefaultVertex defaultVertex = graphData.get(key);
            VertexStatusForInt vertexStatusForInt = (VertexStatusForInt) graphSsspStatus.get(key);
            if (vertexStatusForInt != null) {
              if (vertexStatusForInt.getValue() != Integer.MAX_VALUE) {
                if (defaultVertex.getId().equals(sourceVertexGlobal)) {
                  ArrayList<DefaultEdge> arrayList  = (ArrayList<DefaultEdge>)
                      defaultVertex.getEdges();
                  for (DefaultEdge defaultEdge : arrayList) {
                    String key1 = defaultEdge.getTargetVertexId();
                    Integer edgevalue = defaultEdge.getValue();
                    context.write("keyedreduce", key1, new int[]{edgevalue});
                  }

                } else {
                  if (defaultVertex.getValue() > vertexStatusForInt.getValue()) {
                    defaultVertex.setValue(vertexStatusForInt.getValue());
                    ArrayList<DefaultEdge> arrayList  = (ArrayList<DefaultEdge>)
                        defaultVertex.getEdges();
                    for (DefaultEdge defaultEdge : arrayList) {
                      String key1 = defaultEdge.getTargetVertexId();
                      Integer edgevalue = defaultEdge.getValue();

                      context.write("keyedreduce", key1, new int[]
                          {edgevalue + defaultVertex.getValue()});

                    }
                  }

                }
              }

            }
            count++;
          }
        } else {
          if (context.writeEnd("keyedreduce", "taskend", new int[]{0})) {
            count = 0;

          }
        }
      } else {
        globaliterationStatus = false;
        Iterator it = graphData.entrySet().iterator();
        while (it.hasNext()) {
          Map.Entry pair = (Map.Entry) it.next();
          String key = (String) pair.getKey();
          DefaultVertex defaultVertex = (DefaultVertex) pair.getValue();
          System.out.println("vertex: " + key + "value: " + defaultVertex.getValue());

        }
        context.writeEnd("keyedreduce", "taskend", new int[]{0});
      }

    }
  }

  private class Task2 extends ComputeTask {

    @Override
    public void calculation(Iterator iterator) {
      HashMap<String, VertexStatus> hashMap = new HashMap<>();
      while (iterator.hasNext()) {
        Object next = iterator.next();
        if (next instanceof Tuple) {
          Tuple kc = (Tuple) next;
          if (!kc.getKey().equals("taskend")) {
            VertexStatusForInt vertexStatusForInt = new VertexStatusForInt();
            vertexStatusForInt.setId((String) kc.getKey());
            vertexStatusForInt.setValue(((int[]) kc.getValue())[0]);

            hashMap.put((String) kc.getKey(), vertexStatusForInt);
            context.write("all-reduce", hashMap);
          } else {
            context.write("all-reduce", hashMap);
          }


        }
      }
    }
  }

  private class RedFun extends ReductionFunction {

    @Override
    public Object onMessage(Object object1, Object object2) {
      List<Integer> list = new ArrayList<Integer>();
      int[] data1 = (int[]) object1;
      int[] data2 = (int[]) object2;

      list.add(data1[0]);
      list.add(data2[0]);
      int x = list.indexOf(Collections.min(list));
      return new int[]{list.get(x)};
    }
  }
}

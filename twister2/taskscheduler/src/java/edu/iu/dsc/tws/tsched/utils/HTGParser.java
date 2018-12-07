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
package edu.iu.dsc.tws.tsched.utils;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.Vertex;
import edu.iu.dsc.tws.task.graph.htg.HierarchicalTaskGraph;

public class HTGParser {

  private static final Logger LOG = Logger.getLogger(HTGParser.class.getName());

  private HierarchicalTaskGraph hierarchicalTaskGraph;
  private List<DataFlowTaskGraph> taskgraphList = new LinkedList<>();

  public HTGParser(HierarchicalTaskGraph graph) {
    this.hierarchicalTaskGraph = graph;
  }

  public List<DataFlowTaskGraph> hierarchicalTaskGraphParse() {

    Set<DataFlowTaskGraph> taskGraphSet = hierarchicalTaskGraph.getTaskGraphSet();

    //To parse the hierarchical dataflow task graph and its vertex names.
    Iterator<DataFlowTaskGraph> iterator = taskGraphSet.iterator();
    while (iterator.hasNext()) {
      DataFlowTaskGraph dataFlowTaskGraph = iterator.next();
      Set<Vertex> vertexSet = dataFlowTaskGraph.getTaskVertexSet();
      Iterator<Vertex> vertexIterator = vertexSet.iterator();
      LOG.fine("Dataflow task graph name:" + dataFlowTaskGraph.getTaskGraphName() + "\t"
          + "(" + dataFlowTaskGraph.getOperationMode() + ")\t"
          + vertexSet);
      while (vertexIterator.hasNext()) {
        LOG.fine("Vertex names:" + vertexIterator.next().getName());
      }
    }

    for (DataFlowTaskGraph dataFlowTaskGraph : taskGraphSet) {
      if (hierarchicalTaskGraph.inDegreeOfTaskGraph(dataFlowTaskGraph) == 0) {
        taskgraphList.add(dataFlowTaskGraph);
      } else if ((hierarchicalTaskGraph.incomingTaskGraphEdgesOf(dataFlowTaskGraph).size() == 1)
          && (hierarchicalTaskGraph.outgoingTaskGraphEdgesOf(dataFlowTaskGraph).size() == 0)) {
        taskgraphList.add(dataFlowTaskGraph);
      }
    }

    for (int i = 0; i < taskgraphList.size(); i++) {
      DataFlowTaskGraph dataFlowTaskGraph = taskgraphList.get(i);
      LOG.fine("Dataflow Task Graph and Type:" + dataFlowTaskGraph + "\t"
          + dataFlowTaskGraph.getOperationMode());
    }

    return taskgraphList;
  }
}

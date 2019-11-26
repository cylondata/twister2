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
package edu.iu.dsc.tws.tset.env;

import java.util.Set;

import edu.iu.dsc.tws.api.compute.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.compute.executor.IExecutor;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.task.graph.GraphBuilder;
import edu.iu.dsc.tws.task.impl.TaskExecutor;
import edu.iu.dsc.tws.tset.Buildable;
import edu.iu.dsc.tws.tset.sets.BuildableTSet;

/**
 * TSet build context holds references for executed tset, build sequence of that tset, compute
 * graph
 */
public class BuildContext {
  /**
   * Unique id for a tset graph execution
   */
  private String buildId;

  /**
   * Set of root Tsets of the tset subgraph
   */
  private Set<BuildableTSet> rootTBases;

  /**
   * The order in which the tsets and tlinks will be built
   */
  private Set<TBase> buildSequence;

  /**
   * Batch or streaming mode
   */
  private OperationMode operationMode;

  /**
   * Compute graph based on the build order
   */
  private ComputeGraph computeGraph;

  /**
   * Execution plan based on the compute graph
   */
  private ExecutionPlan executionPlan;

  /**
   * Current executor, this can be null in the beginning
   */
  private IExecutor executor;

  public BuildContext(String bId, Set<BuildableTSet> roots, Set<TBase> buildSeq,
                      OperationMode opMode) {
    this.buildId = bId;
    this.rootTBases = roots;
    this.buildSequence = buildSeq;
    this.operationMode = opMode;
  }

  public ComputeGraph getComputeGraph() {
    if (computeGraph == null) {
      throw new RuntimeException("Compute graph null. TBaseBuildContext is not built!");
    }
    return computeGraph;
  }

  public ExecutionPlan getExecutionPlan() {
    if (executionPlan == null) {
      throw new RuntimeException("Compute graph null. TBaseBuildContext is not built!");
    }
    return executionPlan;
  }

  public String getBuildId() {
    return buildId;
  }

  public Set<BuildableTSet> getRoots() {
    return rootTBases;
  }

  public Set<TBase> getBuildSequence() {
    return buildSequence;
  }

  public IExecutor getExecutor() {
    return executor;
  }

  public boolean build(TaskExecutor taskExecutor) {
    if (computeGraph == null || executionPlan == null) {
      GraphBuilder graphBuilder = GraphBuilder.newBuilder();
      graphBuilder.operationMode(operationMode);

      // building the individual TBases
      for (TBase node : buildSequence) {
        // here, build seq is required for tlinks to filter out nodes that are relevant to this
        // particular build sequence
        ((Buildable) node).build(graphBuilder, buildSequence);
      }

      computeGraph = graphBuilder.build();
      computeGraph.setGraphName(buildId);

      executionPlan = taskExecutor.plan(computeGraph);

      // here we create the executor for this graph, it will be used to execut the graph
      executor = taskExecutor.createExecution(computeGraph, executionPlan);

      return true;
    }
    // BuildContext has already been built. Exiting!
    return false;
  }
}

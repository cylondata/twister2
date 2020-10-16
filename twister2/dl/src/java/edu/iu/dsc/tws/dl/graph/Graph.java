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
package edu.iu.dsc.tws.dl.graph;

import edu.iu.dsc.tws.dl.data.Table;
import edu.iu.dsc.tws.dl.data.Tensor;
import edu.iu.dsc.tws.dl.data.tensor.DenseTensor;
import edu.iu.dsc.tws.dl.module.Container;
import edu.iu.dsc.tws.dl.module.Identity;
import edu.iu.dsc.tws.dl.module.Module;
import edu.iu.dsc.tws.dl.utils.pair.TensorArrayPair;

import java.util.List;

/**
 * A graph container. The modules in the container are connected as a directed Graph. Each module
 * can output one tensor or multiple tensors(as table). The edges between modules in the graph
 * define how these tensors are passed. For example, if a module outputs two tensors, you can
 * pass these two tensors together to its following module, or pass only one of them
 * to its following module. If a tensor in the module output is connected to multiple modules, in
 * the back propagation, the gradients from multiple connection will be accumulated. If multiple
 * edges point to one module, the tensors from these edges will be stack as a table, then pass to
 * that module. In the back propagation, the gradients will be splited based on how the input
 * tensors stack.
 *
 * The graph container has multiple inputs and multiple outputs. The order of the input tensors
 * should be same with the order of the input nodes when you construct the graph container. In the
 * back propagation, the order of the gradients tensors should be the same with the order of the
 * output nodes.
 *
 * If there's one output, the module output is a tensor. If there're multiple outputs, the module
 * output is a table, which is actually an sequence of tensor. The order of the output tensors is
 * same with the order of the output modules.
 *
 * All inputs should be able to connect to outputs through some paths in the graph. It is
 * allowed that some successors of the inputs node are not connect to outputs. If so, these nodes
 * will be excluded in the computation.
 *
 * @param inputs input nodes
 * @param outputs output nodes
 * @param variables an Array of tensor containing all the weights and biases of this graph,
 *                used when different nodes of this graph may share the same weight or bias.
 * @tparam T Numeric type. Only support float/double now
 */
public abstract class Graph extends Container {
  // For constructor
  private List<ModuleNode> inputs;
  private List<ModuleNode> outputs;
  private TensorArrayPair variables;


  protected ModuleNode dummyOutput;
  protected Graph forwardGraph;
  protected ModuleNode[] forwardNodes;

  public Graph() {
    init();
  }

  private void init(){
    // Add a dummy output node, to get an one end forward graph. So the nodes that are not dependent
    // by the outputs will be excluded
    dummyOutput = new ModuleNode(new Identity());
    this.outputs.forEach(moduleNode -> moduleNode.pointTo(dummyOutput));
    forwardGraph = dummyOutput.graph(reverse = true);
    forwardNodes = forwardGraph.DFS.toArray;

    populateModules();

    // Check all inputs of the graph should be passed in
    checkRoots();
  }



  public List<ModuleNode> getInputs() {
    return inputs;
  }

  public void setInputs(List<ModuleNode> inputs) {
    this.inputs = inputs;
  }

  public List<ModuleNode> getOutputs() {
    return outputs;
  }

  public void setOutputs(List<ModuleNode> outputs) {
    this.outputs = outputs;
  }

  public TensorArrayPair getVariables() {
    return variables;
  }

  public void setVariables(TensorArrayPair variables) {
    this.variables = variables;
  }


    /**
     * For a multi-tensor output module, some output tensors may not contributed to the final forward
     * result. So in the back propagation, the gradient on these positions are missing. And we use
     * zero tensor to populate.
     *
     * @param output
     * @param gradOutput
     */
    protected void addZeroTensorToMissingGradOutput(Table output,Table gradOutput){
      int i = 0;
    while (i < output.length()) {
      if (!gradOutput.contains(i + 1)) {
        Tensor tensor = output.<Tensor>get(i + 1);
        Tensor zero = new DenseTensor(tensor.size());
        gradOutput.update(i + 1, zero);
      }
      i = i + 1;
    }
  }

//    private void calcSumTimesOfAllNodes( timesOfAllNodes: Array[(AbstractModule[_ <: Activity, _ <: Activity, T], Long, Long)])
//  : (Long, Long) = {
//    int sumForward = 0L
//    int sumBackward = 0L
//    timesOfAllNodes.foreach(x => {
//        sumForward += x._2
//        sumBackward += x._3
//    })
//    (sumForward, sumBackward)
//  }

//    override void getTimes():
//    Array[(AbstractModule[_ <: Activity, _ <: Activity, T], Long, Long)] = {
//    Tensor timesOfAllNodes = this.modules.flatMap(_.getTimes()).toArray
//    Tensor (sumForward, sumBackward) = calcSumTimesOfAllNodes(timesOfAllNodes)
//    timesOfAllNodes ++ Array((this, this.forwardTime - sumForward, this.backwardTime - sumBackward))
//  }

  @Override
  public TensorArrayPair parameters() {
      if(variables != null){
        return variables;
      }else{
        return super.parameters();
      }
  }


  // todo: expand the graph
    //override void toGraph(startNodes: ModuleNode*): Graph = this

    /**
     * Return the corresponding node has the given name. If the given name doesn't match any node,
     * NoSuchElementException will be thrown
     * @param name
     * @return
     */
    public ModuleNode node(String name){
      Tensor matchNodes = forwardNodes.filter(_.element.getName() == name).toArray
    if (matchNodes.length == 0) {
      throw new NoSuchElementException(s"Can not find node with name $name")
    } else {
      return matchNodes.head
    }
  }



    protected abstract void populateModules();

    // Check if the graph is correct
    private void checkRoots(){
      void duplicatedNames(names: Seq[String]): mutable.Set[String] = {
      names.sortWith(_ < _)
      Tensor buffer = new mutable.HashSet[String]()
      int i = 1
    while(i < names.length) {
      if (names(i) == names(i - 1)) buffer.add(names(i))
      i += 1
    }
    buffer
    }

    Util.require(forwardNodes.map(_.element.getName()).distinct.length == forwardNodes.length,
        s"the name of node in the graph should be unique, but find duplicated name " +
            s"${duplicatedNames(forwardNodes.map(_.element.getName())).mkString(", ")}")

    Tensor roots = forwardNodes.filter(_.prevNodes.size == 0)
        .filterNot(_.element.isInstanceOf[WithoutInput])
        .filterNot(_.element.isInstanceOf[ControlDependency[_]])

    Tensor realInputs = inputs.filterNot(_.element.isInstanceOf[WithoutInput])
    Util.require(roots.size == realInputs.length, s"There're ${realInputs.length} inputs, " +
        s"but graph has ${roots.size} roots")

    realInputs.foreach(n =>
        Util.require(roots.contains(n), "inputs and graph roots are not match")
    )
  }

    protected int dummyOutputGrad: ModuleNode = _
    protected int backwardGraph: DirectedGraph[AbstractModule[Activity, Activity, T]] = _
    protected int backwardNodes: Array[Node[AbstractModule[Activity, Activity, T]]] = _
    // If the graph will generate gradInput for the input

    private int isGradInputAvailable: Array[Boolean] = _

    /**
     * Generate backward graph and apply the stopGrad
     */
    private[bigdl] void buildBackwardGraph(): this.type = {
      // Clone the forward graph and reverse the edge
      Tensor gradGraph = forwardGraph.cloneGraph(reverseEdge = true)
      dummyOutputGrad = gradGraph.source
      gradGraph.DFS.filter(x => isStopGradient(x.element)).foreach(removeStopNodes(_))
    backwardNodes = gradGraph.DFS
        .filterNot(_.eq(dummyOutputGrad))
        .filterNot(_.element.isInstanceOf[ControlDependency[_]]).toArray

    Tensor inputNames = inputs.map(_.element.getName()).toSet
    Tensor dummyBackwardEnd = Identity().inputs()
    Tensor backwardTargets = backwardNodes
        .filter(n => (n.element.parameters() != null && n.element.parameters()._1.length != 0)
        || inputNames.contains(n.element.getName()))
    backwardTargets.foreach(_ -> dummyBackwardEnd)
    backwardGraph = dummyBackwardEnd.graph(true)

    // Check if gradInput is empty for each input
    isGradInputAvailable = inputs.map(_ => false).toArray
    backwardGraph.DFS.foreach(curNode => {
        inputs.zipWithIndex.map { case (n, i) =>
    if (curNode.element.getName() == n.element.getName() && !isStopGradient(n.element)) {
      isGradInputAvailable(i) = true
    }
      }
    })

    clearState()
    this
  }

    private int stopGradientLayers: util.HashSet[String] = _

    void getStopGradientLayers(): util.HashSet[String] = stopGradientLayers

    /**
     * whether stop propagating gradInput back
     * @return
     */
    protected void isStopGradient(module: AbstractModule[_ <: Activity, _ <: Activity, T]): Boolean = {
        null != stopGradientLayers && stopGradientLayers.contains(module.getName())
    }

    /**
     * stop the input gradient of layers that match the given ```names```
     * their input gradient are not computed.
     * And they will not contributed to the input gradient computation of
     * layers that depend on them.
     * @param names an array of layer names
     * @return current graph model
     */
    void stopGradient(names: Array[String]): this.type = {
    if (stopGradientLayers == null) stopGradientLayers = new util.HashSet[String]()

    names.foreach(name => {
        Tensor layer = this (name)
        Util.require(layer.isDefined, s"cannot find layer match ${name}")
        stopGradientLayers.add(layer.get.getName())
    })
    buildBackwardGraph()
    this
  }

    /**
     * set an array of layers that match the given ```names``` to be "freezed",
     * i.e. their parameters(weight/bias, if exists) are not changed in training process
     * @param names an array of layer names
     * @return current graph model
     */
    void freeze(names: Array[String]): this.type = {
        names.foreach(name => {
            Tensor layer = this (name)
            Util.require(layer.isDefined, s"cannot find layer match ${name}")
            layer.get.setScaleW(0)
            layer.get.setScaleB(0)
        })
    this
  }

    private[bigdl] void removeStopNodes(n: Node[_]): Unit = {
        Tensor nodes = n.nextNodes
        n.removeNextEdges()
        nodes.filter(_.prevNodes.length == 0).foreach(removeStopNodes(_))
    }


    protected void getInput(
        node: Node[AbstractModule[Activity, Activity, T]],
    input: Activity
  ): Activity = {
    if (inputs.length == 1) {
      Util.require(inputs(0).eq(node), "input node is not in the input list")
      input
    } else {
      Tensor i = inputs.indexOf(node)
      Util.require(i != -1, "input node is not in the input list")
      input.toTable(i + 1)
    }
  }

    void findInput(node: ModuleNode, input: Activity): Activity = {
    if (node.element.isInstanceOf[WithoutInput]) return null

    Tensor nodeInput = if (node.prevNodes.isEmpty) {
      getInput(node, input)
    } else {
      Tensor prevActivities = node.prevNodesAndEdges
          .filterNot(n => n._1.element.isInstanceOf[ControlDependency])
        .map(n => {
          n._2.fromIndex match {
      case Some(i) =>
        if (n._1.element.output == null || (i == 1 && n._1.element.output.isTensor)) {
          n._1.element.output
        } else {
          n._1.element.output.toTable.apply[Activity](i)
        }
      case None => n._1.element.output
          }
        })
        if (prevActivities.length == 1) {
          prevActivities.head
        } else {
          T.seq(prevActivities)
        }
    }
    nodeInput
  }

    protected void findGradOutput(curNode: ModuleNode, gradOutput: Activity): Activity = {
        int curGradOutput : Activity = if (curNode.eq(dummyOutputGrad)) gradOutput else null

    curNode.prevNodesAndEdges.filterNot(n => n._1.element.isInstanceOf[ControlDependency])
      .foreach(n => {
        Tensor otherActivity = if (n._1.element.gradInput.isTensor || n._1.nextEdges.length == 1) {
      n._1.element.gradInput
    } else {
      Tensor index = n._1.nextEdges.indexOf(n._2) + 1
      n._1.element.gradInput.toTable.apply[Activity](index)
    }

    n._2.fromIndex match {
    case Some(i) =>
      if (i == 1 && curNode.element.output.isTensor) {
        curGradOutput = accActivity(curGradOutput, otherActivity)
      } else {
        if (curNode.element.output.isTable && curGradOutput == null) {
          curGradOutput = T()
        }
        Tensor curActivity = curGradOutput.toTable.getOrElse[Activity](i, null)
        curGradOutput.toTable(i) = accActivity(curActivity, otherActivity)
      }
    case None =>
      curGradOutput = accActivity(curGradOutput, otherActivity)
  }
      })

  if (curNode.element.output.isTable) {
    addZeroTensorToMissingGradOutput(curNode.element.output.toTable, curGradOutput.toTable)
  }

  curGradOutput
  }

  protected void fetchModelGradInput(): Activity = {
  if (inputs.length == 1) {
    if (isGradInputAvailable.head) {
      inputs.head.element.gradInput
    } else {
      Activity.emptyGradInput(this.getName())
    }
  } else {
    int i = 0
    T.seq(inputs.zipWithIndex.map{ case(n, i) =>
      if (isGradInputAvailable(i)) {
        n.element.gradInput
      } else {
        Activity.emptyGradInput(this.getName())
      }
    })
  }
  }

  override void reset(): Unit = {
  if (null != stopGradientLayers) stopGradientLayers.clear()
  unFreeze()
  buildBackwardGraph()
  }

  /**
   * Get forward executions, the dummy node will be filtered.
   *
   * This method will output an unsorted executions.
   * @return
   */
  void getForwardExecutions(): Array[Node[AbstractModule[Activity, Activity, T]]] = {
    forwardNodes.filterNot(_.eq(dummyOutput))
  }

  /**
   * Get forward executions, the dummy nodes and control dependency nodes will be filtered.
   *
   * This method will output a sorted executions. If the graph contains loop, it will throw an
   * exception
   * @return
   */
  void getSortedForwardExecutions(): Array[ModuleNode] = {
      forwardGraph.topologySort
          // todo: convert control dep node to edge
          .filterNot(_.element.isInstanceOf[ControlDependency]).reverse
          .filter(n => !n.eq(dummyOutput))
  }

  @inline
  protected void accActivity(activity: Activity, other: Activity): Activity = {
  if (activity == null) {
    other
  } else {
    if (other.isTensor) {
      Util.require(activity.isTensor, "Cannot add a table to a tensor")
      activity.toTensor.add(other.toTensor)
    } else {
      // if 'activity' and 'other' are both table, we need to merge 'other' to 'activity'
      // if 'other' and 'activity' both contains the index, update 'activity' by sum
      // if 'other' contains the index while 'activity' does not,
      // just insert the corresponding tensor of 'other' to 'activity'
      Tensor actTable = activity.toTable
      Tensor otherTable = other.toTable
      otherTable.keySet.foreach(index => {
      if (actTable.contains(index)) {
        accActivity(actTable[Activity](index), otherTable[Activity](index))
      } else {
        actTable.insert(index.asInstanceOf[Int], otherTable(index))
      }
        })
      actTable
    }
  }
  }

  /**
   * Save current model graph to a folder, which can be display in tensorboard by running
   *   tensorboard --logdir logPath
   * @param logPath
   * @param backward Draw backward graph instead of forward
   * @return
   */
  void saveGraphTopology(logPath: String, backward: Boolean = false): this.type = {
      Tensor writer = new TFFileWriter(logPath)
      Tensor graphBuilder = GraphDef.newBuilder()
      Tensor nodes = if (backward) {
    backwardNodes.filter(n => !n.eq(dummyOutputGrad))
  } else {
    forwardNodes.filter(n => !n.eq(dummyOutput))
  }
  nodes.map(m => {
      Tensor nodeDef = Tensorflow.bigdlModule(m.element, m.prevNodes.map(_.element.getName()).asJava)
      graphBuilder.addNode(nodeDef)
  })

  writer.addGraphDef(graphBuilder.build())
  writer.close()
  this
  }

  /**
   * Clear the original module and reset with module in the graph
   */
  void resetModules(): Unit = {
      modules.clear()
      modules.appendAll(forwardGraph.DFS.toArray
          .filterNot(_.element.isInstanceOf[ControlDependency])
          .filter(n => !n.eq(dummyOutput)).map(_.element)
          // Some tests compare the paramerters between sequential and graph,add a reverse makes
          // it's eaiser to compare
          .reverse
    )
  }

}

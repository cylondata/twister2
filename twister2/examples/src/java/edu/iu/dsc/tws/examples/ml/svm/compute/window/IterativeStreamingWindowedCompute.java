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
package edu.iu.dsc.tws.examples.ml.svm.compute.window;

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.TaskContext;
import edu.iu.dsc.tws.api.task.graph.OperationMode;
import edu.iu.dsc.tws.examples.ml.svm.exceptions.MatrixMultiplicationException;
import edu.iu.dsc.tws.examples.ml.svm.exceptions.NullDataSetException;
import edu.iu.dsc.tws.examples.ml.svm.util.BinaryBatchModel;
import edu.iu.dsc.tws.examples.ml.svm.util.MLUtils;
import edu.iu.dsc.tws.examples.ml.svm.util.SVMJobParameters;
import edu.iu.dsc.tws.examples.ml.svm.util.TrainedModel;
import edu.iu.dsc.tws.task.window.api.IWindowMessage;
import edu.iu.dsc.tws.task.window.collectives.ProcessWindow;
import edu.iu.dsc.tws.task.window.function.ProcessWindowedFunction;

public class IterativeStreamingWindowedCompute extends ProcessWindow<double[]> {

  private static final long serialVersionUID = -2951513581887018830L;

  private static final Logger LOG = Logger.getLogger(IterativeStreamingWindowedCompute.class
      .getName());

  private OperationMode operationMode;

  private SVMJobParameters svmJobParameters;

  private BinaryBatchModel binaryBatchModel;

  private String modelName;

  private TrainedModel trainedModel;


  private static int counter = 0;

  private boolean debug = false;

  public IterativeStreamingWindowedCompute(
      ProcessWindowedFunction<double[]> processWindowedFunction, OperationMode operationMode) {
    super(processWindowedFunction);
    this.operationMode = operationMode;
  }

  public IterativeStreamingWindowedCompute(
      ProcessWindowedFunction<double[]> processWindowedFunction, OperationMode operationMode,
      SVMJobParameters svmJobParameters, BinaryBatchModel binaryBatchModel) {
    super(processWindowedFunction);
    this.operationMode = operationMode;
    this.svmJobParameters = svmJobParameters;
    this.binaryBatchModel = binaryBatchModel;
  }

  public IterativeStreamingWindowedCompute(
      ProcessWindowedFunction<double[]> processWindowedFunction, OperationMode operationMode,
      SVMJobParameters svmJobParameters, BinaryBatchModel binaryBatchModel, String modelName) {
    super(processWindowedFunction);
    this.operationMode = operationMode;
    this.svmJobParameters = svmJobParameters;
    this.binaryBatchModel = binaryBatchModel;
    this.modelName = modelName;
  }

  public IterativeStreamingWindowedCompute(
      ProcessWindowedFunction<double[]> processWindowedFunction) {
    super(processWindowedFunction);
  }

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    super.prepare(cfg, ctx);
  }

  @Override
  public boolean process(IWindowMessage<double[]> windowMessage) {
    if (debug) {
      LOG.info(String.format("[%d] Message List Size : %d ", counter++, windowMessage.getWindow()
          .size()));
    }
    try {
      trainedModel = MLUtils.runIterativeSGDSVM(windowMessage.getWindow(), this.svmJobParameters,
          this.binaryBatchModel, this.modelName);
    } catch (NullDataSetException e) {
      LOG.severe(String.format("NullDataSetException : %s", e.getMessage()));
    } catch (MatrixMultiplicationException e) {
      LOG.severe(String.format("MatrixMultiplicationException : %s", e.getMessage()));
    }
    context.write("window-sink-edge", trainedModel.getW());
    return true;
  }

  @Override
  public boolean processLateMessages(IMessage<double[]> lateMessage) {
    return true;
  }

}

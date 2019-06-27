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

import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.graph.OperationMode;
import edu.iu.dsc.tws.task.window.api.IWindowMessage;
import edu.iu.dsc.tws.task.window.collectives.ProcessWindow;
import edu.iu.dsc.tws.task.window.function.ProcessWindowedFunction;

public class IterativeStreamingWindowedCompute extends ProcessWindow<double[]> {

  private static final long serialVersionUID = -2951513581887018830L;

  private static final Logger LOG = Logger.getLogger(IterativeStreamingWindowedCompute.class
      .getName());

  private OperationMode operationMode;

  private static int counter = 0;

  public IterativeStreamingWindowedCompute(
      ProcessWindowedFunction<double[]> processWindowedFunction, OperationMode operationMode) {
    super(processWindowedFunction);
    this.operationMode = operationMode;
  }

  public IterativeStreamingWindowedCompute(
      ProcessWindowedFunction<double[]> processWindowedFunction) {
    super(processWindowedFunction);
  }

  @Override
  public boolean process(IWindowMessage<double[]> windowMessage) {
    LOG.info(String.format("[%d] Message List Size : %d ", counter++, windowMessage.getWindow()
        .size()));
    return true;
  }

  @Override
  public boolean processLateMessages(IMessage<double[]> lateMessage) {
    return true;
  }


}

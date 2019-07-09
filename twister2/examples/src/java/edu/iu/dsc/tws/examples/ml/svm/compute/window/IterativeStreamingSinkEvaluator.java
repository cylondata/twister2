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
import edu.iu.dsc.tws.api.task.nodes.BaseSink;

/**
 * This is not a windowed Sink but this is the sink used after a windowed compute
 */
public class IterativeStreamingSinkEvaluator extends BaseSink<Double> {
  private static final long serialVersionUID = 1576464625318900125L;

  private int count = 0;

  private static final Logger LOG = Logger.getLogger(IterativeStreamingSinkEvaluator.class
      .getName());

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    super.prepare(cfg, ctx);
  }

  @Override
  public boolean execute(IMessage<Double> message) {
    LOG.info(String.format("Aggregated[%d] Accuracy : %f", count++,
        message.getContent() / (double) context.getParallelism()));
    return true;
  }
}

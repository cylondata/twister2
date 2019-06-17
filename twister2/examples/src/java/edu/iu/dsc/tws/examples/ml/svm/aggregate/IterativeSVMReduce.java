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
package edu.iu.dsc.tws.examples.ml.svm.aggregate;

import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.Collector;
import edu.iu.dsc.tws.dataset.DataPartition;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class IterativeSVMReduce extends BaseSink implements Collector {

  private static final long serialVersionUID = -254264120110286748L;

  private static final Logger LOG = Logger.getLogger(IterativeSVMReduce.class.getName());

  private double[] object;

  private boolean debug = false;

  private boolean status = false;

  private OperationMode operationMode;

  @Override
  public DataPartition<?> get() {
    return null;
  }

  @Override
  public boolean execute(IMessage content) {
    return false;
  }
}

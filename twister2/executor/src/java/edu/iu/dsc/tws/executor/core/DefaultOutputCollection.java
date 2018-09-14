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
package edu.iu.dsc.tws.executor.core;

import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.OutputCollection;

public class DefaultOutputCollection implements OutputCollection {
  private static final Logger LOG = Logger.getLogger(DefaultOutputCollection.class.getName());

  private BlockingQueue<IMessage> outQueue;

  private int count = 0;

  public DefaultOutputCollection(BlockingQueue<IMessage> outQueue) {
    this.outQueue = outQueue;
  }

  @Override
  public boolean collect(String edge, IMessage m) {
    boolean offer = this.outQueue.offer(m);
    if (offer) {
      count++;
    }
    return offer;
  }
}

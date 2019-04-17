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
package edu.iu.dsc.tws.comms.dfw.io.partition;

import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.dfw.io.TargetPartialReceiver;

/**
 * This is the partial receiver for the partition operation.
 * Partial receiver is only going to get called for messages going to other destinations
 * We have partial receivers for each actual source, So even if the message is going to be forwarded
 * to a task within the same worker the message will still go through the partial receiver.
 */
public class PartitionPartialReceiver extends TargetPartialReceiver {
  private static final Logger LOG = Logger.getLogger(PartitionPartialReceiver.class.getName());
}

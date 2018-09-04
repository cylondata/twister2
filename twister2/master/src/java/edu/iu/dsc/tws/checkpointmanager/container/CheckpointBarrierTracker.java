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
package edu.iu.dsc.tws.checkpointmanager.container;

import java.io.IOException;
import java.util.logging.Logger;

import edu.iu.dsc.tws.task.api.INode;

public class CheckpointBarrierTracker implements CheckpointBarrierHandler {
  private static final Logger LOG = Logger.getLogger(CheckpointBarrierTracker.class.getName());

  @Override
  public void registerCheckpointEventHandler(INode task) {

  }

  @Override
  public void cleanup() throws IOException {

  }

  @Override
  public boolean isEmpty() {
    return false;
  }
}

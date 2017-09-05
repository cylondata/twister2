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
package edu.iu.dsc.tws.executor.api.observer;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.executor.model.Task;

/**
 * Created by vibhatha on 8/25/17.
 */
public class GetCountTaskAddedListener implements ITaskAddedListener {

  private static final Logger LOGGER = Logger.getLogger( GetCountTaskAddedListener.class.getName() );
  private static AtomicLong taskAddedCount = new AtomicLong(0);

  @Override
  public void onTaskAdded(Task task) {

  }

  @Override
  public void updateTaskAdded(Task task) {
    taskAddedCount.incrementAndGet();
    LOGGER.log( Level.FINE, "\n Total Tasks : {0} ", taskAddedCount);
    System.out.println("=================================");
    System.out.println("\n Total Tasks : "+taskAddedCount);
    System.out.println("=================================");
  }
}

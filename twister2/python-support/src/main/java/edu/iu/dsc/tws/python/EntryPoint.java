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
package edu.iu.dsc.tws.python;

import java.util.concurrent.Semaphore;

import edu.iu.dsc.tws.api.compute.modifiers.Closable;

public abstract class EntryPoint implements Closable {

  private Semaphore wait = new Semaphore(0);

  @Override
  public void close() {
    wait.release();
  }

  public void waitForCompletion() throws InterruptedException {
    this.wait.acquire();
  }
}

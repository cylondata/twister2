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

package edu.iu.dsc.tws.tset.cdfw;

import edu.iu.dsc.tws.task.cdfw.BaseDriver;
import edu.iu.dsc.tws.task.cdfw.CDFWEnv;

/**
 * The base driver class for TSet API when using connected dataflow
 */
public abstract class StreamTSetBaseDriver extends BaseDriver {
  @Override
  public void execute(CDFWEnv exec) {
    StreamTSetCDFWEnvironment env = new StreamTSetCDFWEnvironment(exec);
    execute(env);
  }

  abstract void execute(StreamTSetCDFWEnvironment env);
}

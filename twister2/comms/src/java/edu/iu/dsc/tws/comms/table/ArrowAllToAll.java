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
package edu.iu.dsc.tws.comms.table;

/**
 * This class gets values as normal format and converts them into a row format before invoking the
 * communication.
 */
public class ArrowAllToAll {
  public boolean insert(ArrowTable table, int target) {
    return false;
  }

  public boolean isComplete() {
    return false;
  }

  public void finish(int source) {

  }

  public void close() {

  }
}

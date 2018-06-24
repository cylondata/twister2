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
package edu.iu.dsc.tws.checkpointmanager.state;

import java.util.HashMap;

public class Snapshot {

  private HashMap<String, Object> stateMap = new HashMap<>();

  public Object getState(String stateName) {
    return stateMap.get(stateName);
  }

  public void addState(String stateName, Object object) {
    stateMap.put(stateName, object);
  }
}

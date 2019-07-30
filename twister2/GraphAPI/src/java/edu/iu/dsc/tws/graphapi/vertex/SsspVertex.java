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
package edu.iu.dsc.tws.graphapi.vertex;


import java.util.HashMap;

public class SsspVertex {

  private String id;
  private boolean status;
  private int value;

  public void setValue(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public void setStatus(boolean status) {
    this.status = status;
  }

  public boolean isStatus() {
    return status;
  }

  private HashMap<String, Integer> hashMap = new HashMap<>();

  public void setHashMap(HashMap<String, Integer> hashMap) {
    this.hashMap = hashMap;
  }

  public HashMap<String, Integer> getHashMap() {
    return hashMap;
  }

  public void setId(String id) {
    this.id = id;
  }



  public String getId() {
    return id;
  }


}

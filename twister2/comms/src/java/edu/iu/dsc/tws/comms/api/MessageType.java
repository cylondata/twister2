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
package edu.iu.dsc.tws.comms.api;

//Note: is a new type is added make sure to update the MessageTypeUtils and
// to update edu.iu.dsc.tws.data.memory.utils.DataMessageType
public enum MessageType {
  INTEGER,
  CHAR,
  BYTE,
  MULTI_FIXED_BYTE,
  STRING,
  LONG,
  DOUBLE,
  OBJECT,
  BUFFER,
  EMPTY,
  SHORT;
}

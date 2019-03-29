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
package edu.iu.dsc.tws.api.cdfw;

import com.google.protobuf.Any;

/**
 * Represents an event for Driver
 * <p>
 * todo: check whether this is really required!
 */
public class DriverEvent {
  private DriveEventType type;

  private Any message;

  private int senderId;

  public DriverEvent(DriveEventType type, Any message, int senderId) {
    this.type = type;
    this.message = message;
    this.senderId = senderId;
  }

  public DriveEventType getType() {
    return type;
  }

  public Any getMessage() {
    return message;
  }

  public int getSenderId() {
    return senderId;
  }
}

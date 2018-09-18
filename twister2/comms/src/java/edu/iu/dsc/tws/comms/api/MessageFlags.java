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

public final class MessageFlags {
  private MessageFlags() {
  }

  public static final int LAST = 1 << 30;
  public static final int ORIGIN_SENDER = 1 << 28;
  public static final int ORIGIN_PARTIAL = 1 << 27;
  public static final int END = 1 << 26;
  public static final int BARRIER = 1 << 25;
}

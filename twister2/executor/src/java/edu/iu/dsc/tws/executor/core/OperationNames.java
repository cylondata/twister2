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
package edu.iu.dsc.tws.executor.core;

public final class OperationNames {

  private OperationNames() {
  }

  public static final String REDUCE = "reduce";
  public static final String GATHER = "gather";
  public static final String PARTITION = "partition";
  public static final String ALLGATHER = "allgather";
  public static final String ALLREDUCE = "allreduce";
  public static final String BROADCAST = "broadcast";
  public static final String KEYED_PARTITION = "keyed_partition";
  public static final String KEYED_REDUCE = "keyed_reduce";
  public static final String KEYED_GATHER = "keyed_gather";

}


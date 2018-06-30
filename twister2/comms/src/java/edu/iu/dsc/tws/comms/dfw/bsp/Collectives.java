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
package edu.iu.dsc.tws.comms.dfw.bsp;

import java.nio.Buffer;

import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.Op;
import edu.iu.dsc.tws.comms.dfw.io.MessageSerializer;

import mpi.MPI;
import mpi.MPIException;

public class Collectives {
  private MessageSerializer serializer;

  public Collectives() {
  }

  public Buffer allReduce(Object object, MessageType type,
                          Op op, int len) throws MPIException {
    Buffer buffer = null;
    if (type == MessageType.BUFFER) {
      buffer = (Buffer) object;
    }
    mpi.Op operation = getMPIOperation(op);
    mpi.Datatype datatype = getDataTyle(type);

    MPI.COMM_WORLD.allReduce(buffer, len, datatype, operation);
    return buffer;
  }

  void reduce(Object object, MessageType type, int target, Op op, int len) throws MPIException {
    Buffer buffer = null;
    if (type == MessageType.BUFFER) {
      buffer = (Buffer) object;
    }
    mpi.Op operation = getMPIOperation(op);
    mpi.Datatype datatype = getDataTyle(type);

    MPI.COMM_WORLD.reduce(buffer, len, datatype, operation, target);
  }

  void gather(Object object, MessageType type, int target, int len) throws MPIException {
    Buffer buffer = null;
    if (type == MessageType.BUFFER) {
      buffer = (Buffer) object;
    }
    mpi.Datatype datatype = getDataTyle(type);

    MPI.COMM_WORLD.gather(buffer, len, datatype, target);
  }

  void allGather(Object object, MessageType type, int target, int len) throws MPIException {
    Buffer buffer = null;
    if (type == MessageType.BUFFER) {
      buffer = (Buffer) object;
    }
    mpi.Datatype datatype = getDataTyle(type);
    MPI.COMM_WORLD.allGather(buffer, len, datatype);
  }

  void bcast(Object object, MessageType type, int target, Op op, int len) throws MPIException {
    Buffer buffer = null;
    if (type == MessageType.BUFFER) {
      buffer = (Buffer) object;
    }
    mpi.Op operation = getMPIOperation(op);
    mpi.Datatype datatype = getDataTyle(type);

    MPI.COMM_WORLD.bcast(buffer, len, datatype, target);
  }

  private mpi.Datatype getDataTyle(MessageType type) {
    if (type == MessageType.INTEGER) {
      return MPI.INT;
    } else if (type == MessageType.BYTE || type == MessageType.OBJECT) {
      return MPI.BYTE;
    } else if (type == MessageType.DOUBLE) {
      return MPI.DOUBLE;
    } else if (type == MessageType.LONG) {
      return MPI.LONG;
    } else if (type == MessageType.SHORT) {
      return MPI.SHORT;
    }
    throw new RuntimeException("Un-supported type");
  }

  private mpi.Op getMPIOperation(Op op) {
    if (op == Op.SUM) {
      return MPI.SUM;
    } else if (op == Op.MAX) {
      return MPI.MAX;
    } else if (op == Op.MIN) {
      return MPI.MIN;
    }
    throw new RuntimeException("Un-supported operation");
  }
}

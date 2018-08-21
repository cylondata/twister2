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
package edu.iu.dsc.tws.examples.internal.rsched;

import mpi.MPI;
import mpi.MPIException;

public final class BasicMpiJob {

  private BasicMpiJob() {

  }
  public static void main(String[] args) throws MPIException {


    MPI.Init(args);
    int source;  // Rank of sender
    int dest;    // Rank of receiver
    int tag = 50;  // Tag for messages
    int next;
    int prev;
    int[] message = new int[1];

    int myrank = MPI.COMM_WORLD.getRank();
    int size = MPI.COMM_WORLD.getSize();


    next = (myrank + 1) % size;
    prev = (myrank + size - 1) % size;



    if (0 == myrank) {
      message[0] = 10;

      System.out.println("Process 0 sending " + message[0] + " to rank " + next + " ("
          + size + " processes in ring)");
      MPI.COMM_WORLD.send(message, 1, MPI.INT, next, tag);
    }


    while (true) {
      MPI.COMM_WORLD.recv(message, 1, MPI.INT, prev, tag);
      //System.out.println("Before receive from " + prev);
      if (0 == myrank) {
        --message[0];
        System.out.println("Process 0 decremented value: " + message[0]);
      }

      MPI.COMM_WORLD.send(message, 1, MPI.INT, next, tag);
      if (0 == message[0]) {
        System.out.println("Process " + myrank + " exiting");
        break;
      }
    }
    if (0 == myrank) {
      MPI.COMM_WORLD.recv(message, 1, MPI.INT, prev, tag);
    }

    MPI.Finalize();
  }



 /* public static void main(String[] args) {
    try {

      System.out.printf("inside openmpi: Openmpi has started");
      MPI.Init(args);
      int myrank = MPI.COMM_WORLD.getRank();
      int size = MPI.COMM_WORLD.getSize();
      System.out.println("Hello world from rank " + myrank + " of " + size);

      MPI.Finalize();
    } catch (Exception e) {
      e.printStackTrace();
    }


  }*/
}

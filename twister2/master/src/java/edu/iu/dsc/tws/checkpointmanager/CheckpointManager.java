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
package edu.iu.dsc.tws.checkpointmanager;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.net.tcp.Progress;
import edu.iu.dsc.tws.common.net.tcp.StatusCode;
import edu.iu.dsc.tws.common.net.tcp.request.ConnectHandler;
import edu.iu.dsc.tws.common.net.tcp.request.RRServer;
import edu.iu.dsc.tws.master.JobMaster;
import edu.iu.dsc.tws.proto.checkpoint.Checkpoint;

public class CheckpointManager extends Thread {
  private static final Logger LOG = Logger.getLogger(CheckpointManager.class.getName());

  private RRServer rrServer;

  private Config cfg;

  private Progress looper;

  public CheckpointManager() {

    looper = new Progress();
    rrServer = new RRServer(cfg, "localhost", 6789, looper,
        -2, new ServerConnectHandler());

    TaskBarrierMonitor taskBarrierMonitor = new TaskBarrierMonitor(cfg, rrServer);

    rrServer.registerRequestHandler(Checkpoint.TaskDiscovery.newBuilder(), taskBarrierMonitor);
    rrServer.registerRequestHandler(Checkpoint.BarrierSync.newBuilder(), taskBarrierMonitor);
    rrServer.registerRequestHandler(Checkpoint.CheckpointComplete.newBuilder(), taskBarrierMonitor);

    rrServer.start();

    looper.loop();

    start();
  }

  @Override
  public void run() {

    while (true) {
      looper.loop();
    }

  }

  public class ServerConnectHandler implements ConnectHandler {
    @Override
    public void onError(SocketChannel channel) {
      LOG.severe("Checkpoint manager Server Connect Handler did not start");
    }

    @Override
    public void onConnect(SocketChannel channel, StatusCode status) {
      try {
        LOG.info("Client connected from:" + channel.getRemoteAddress());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void onClose(SocketChannel channel) {
    }
  }
}

Twister2 Communication
======================

.. toctree::
   :maxdepth: 5
   
   communication-model

TCP 
---

TCP functionality is implemented in the common package. It is based on non blocking network IO model
with Java NIO.

We support two modes of communication in the TCP implementation, namely Request/Response mode and
generic messaging mode. 

Edge is an important concept in TCP communication and helps to identify two message streams running
through the same connections. Without edge number, we cannot identify the which message belongs to
which communication.

Each TCP message is preceded by the following header.

.. code-block:: bash

  4 byte integer length \ 4 byte integer edge 


Request Response Mode
----------------------

In Request/Response model, we have a TCP Server and a TCP client which works using requests and
responses. Requests and responses are always protocol buffer messages.

The servers always respond to requests and doesnt initiate any requests. Clients always send 
requests and expect responses.

This model is primarily used for control messages and is not recomended to be used for actual data 
processing messages.

When a message is received, a new byte buffer is allocated according to the length of the message 
and it is filled with the content of the message. This is returned to the application.

The request response mode should use RRServer and RRClient to do communications. In this mode, the 
application must first register protocol buffer message types with the transport. Name of the 
protocol buffer message type and a 32 bit request id is included additionally to the default 
message header.

So each message is preceded by

.. code-block:: bash

  4 byte integer length \ 4 byte integer edge | 32 bytes request id | 4 byte message name length | message name  


When we send a message, a callback is registered to receive the responses. The requests and responces
are matched using the unique request id generated for each message.

Messaging Mode
--------------

In messaging mode, the tcp network sends data buffers in Java ByteBuffer objects. It uses set of 
fixed data buffers to transfer and reveive data.

Each TCP message is preceded by the following header.

.. code-block:: bash

  4 byte integer length \ 4 byte integer edge 



In this mode, receiving buffers must be posted to the tranport and the when it receives a message, 
it will fill these posted buffers with content and return them. This means the size of the buffers
are fixed for sending and receiving.

Here is a psuedo code of how to use the messaging mode.

.. code-block:: java
   :linenos:

  TCPChannel channel = new TCPChannel(...)
  channel.startListening()
  
  channel.startConnections()
  
  // now post the required number of buffers
  TCPMessage rcv = channel.iRecv(recv_buffer, recv_edge, workerId)
  
  // now send messages
  TCPMessage send = channel.iSend(send_buffer, send_edge, workerID)
  
  // now we need to progress the select handler
  channel.progress()
  
  // possibly it will return a message
  if (rcv.isComplete()) {
    // recv message is complete
  }
  
  // possibly it will return a message
  if (send.isComplete()) {
    // send message is complete
  }


Because of the additional information carried in Request/Response mode and the use of protocol
buffers, the general messaging mode is better performing than request / response mode.
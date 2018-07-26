Communication Configuration Paramters
-------------------------------------


MPI Communication
=================

MPI Communication uses 


TCP Communication
=================

TCP functionality is implemented in the common package. It is based on non blocking network IO model
with Java NIO.


We support two modes of communication in the TCP implementation, namely Request/Response mode and
generic messaging mode. 

Request Response Mode
----------------------

In Request/Response model, we have a TCP Server and a TCP client which works using requests and
responses. Requests and responses are always protocol buffer messages.

The servers always respond to requests and doesnt initiate any requests. Clients always send 
requests and expect responses.

This model is primarily used for control messages and is not recomended to be used for actual data 
processing messages.


Messaging Mode
---------------

In messaging mode, the tcp network sends data buffers in Java ByteBuffer objects. It uses set of 
fixed data buffers to transfer and reveive data.




package helloworld

import io.grpc.{Server, ServerBuilder};
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.time.LocalDateTime;

import scala.concurrent.{ExecutionContext, Future}

import protos.helloworld.{HelloRequest, HelloReply, GreeterGrpc}

/**
 * [[https://github.com/grpc/grpc-java/blob/v0.15.0/examples/src/main/java/io/grpc/examples/helloworld/HelloWorldServer.java]]
 */
object HelloWorldServer {
  private val logger = Logger.getLogger(classOf[HelloWorldServer].getName)

  // def main(args: Array[String]): Unit = {
  //   val server = new HelloWorldServer(ExecutionContext.global)
  //   server.start()
  //   server.blockUntilShutdown()
  // }

  private val port = 50051
}

abstract class HelloWorldServer(executionContext: ExecutionContext) { self =>
  private[this] var server: Server = null

  private def start(): Unit = {
    server = ServerBuilder.forPort(HelloWorldServer.port).addService(GreeterGrpc.bindService(new GreeterImpl, executionContext)).build.start
    HelloWorldServer.logger.info("Server started, listening on " + HelloWorldServer.port)
    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      System.err.println("*** server shut down")
    }
  }

  private def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  private def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

  private def getMessage(): String = {
    "Hello~ "
  }

  private class GreeterImpl extends GreeterGrpc.Greeter {
    override def sayHello(req: HelloRequest) = {
      val messageTest = getMessage()
      val reply = HelloReply(message = messageTest + req.name)
      Future.successful(reply)
    }
  }
}
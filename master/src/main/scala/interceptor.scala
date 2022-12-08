package master.server.interceptor

import io.grpc
import io.grpc.{ServerCall, ForwardingServerCallListener, ServerCallHandler, Metadata}
import io.grpc.Grpc.TRANSPORT_ATTR_REMOTE_ADDR
import scala.concurrent.Channel

class ServerCallListener[ReqT](delegate: ServerCall.Listener[ReqT], channel: Channel[String], remoteIpAddress: String) extends ForwardingServerCallListener[ReqT] {

  override protected def delegate: ServerCall.Listener[ReqT] = {
    delegate
  }

  override def onMessage(message: ReqT): Unit = {
    channel.write(remoteIpAddress)
    super.onMessage(message)
  }
}

class ServerInterceptor(channel: Channel[String]) extends grpc.ServerInterceptor {
  override def interceptCall[ReqT, RespT](
    call: ServerCall[ReqT, RespT],
    headers: Metadata,
    next: ServerCallHandler[ReqT, RespT]
  ): ServerCall.Listener[ReqT] = {
    val remoteAddress = call.getAttributes.get(TRANSPORT_ATTR_REMOTE_ADDR)
    val remoteIpAddress = remoteAddress.toString.stripPrefix("/").split(":")(0)
    val listener = next.startCall(call, headers)
    new ServerCallListener(listener, channel, remoteIpAddress)
  }
}

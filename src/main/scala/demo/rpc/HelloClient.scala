package demo.rpc

import com.example.jrpc.nettyrpc.rpc.{HostPort, RpcConfig}
import com.example.srpc.nettyrpc.{RpcEndpointRef, RpcEnv}
import demo.rpc.HelloServer.serverName

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

/**
  * Created by yilong on 2018/4/6.
  */
object HelloClient {
  case class SayHi(msg: String)

  case class SayBye(msg: String)

  def main(args: Array[String]): Unit = {
    syncCall()
  }

  def syncCall() = {
    val rpcEnv: RpcEnv = RpcEnv.create(new RpcConfig(), "localhost", 9091, true)

    val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(new HostPort("localhost", 9091),
      HelloServer.serverName)

    val result = endPointRef.askSync[String](SayBye("neo"), 1000*100)

    println("***********************")
    println(result)
  }
}

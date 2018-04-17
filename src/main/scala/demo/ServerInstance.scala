package demo

import com.example.jrpc.nettyrpc.rpc.RpcConfig
import com.example.srpc.nettyrpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import com.example.jutil.SSConfig
import com.example.lsm.LSMServer
import demo.ClientInstance.KVOp
import demo.rpc.HelloEndpoint
import demo.rpc.HelloServer.serverName

/**
  * Created by yilong on 2018/1/6.
  */
object ServerInstance extends App {
  val host = "localhost"
  val port = 9091
  val serverName = "lsm-server"

  def startApplication() = {

    val memTableManager = LSMServer.build(new SSConfig)
    val memTableManagerThread = new Thread(memTableManager)
    memTableManagerThread.start()

    val rpcEnv: RpcEnv = RpcEnv.create(new RpcConfig(), host, port, false)

    val lsmEndpoint: RpcEndpoint = new LSMServerEndpoint(rpcEnv, memTableManager)

    rpcEnv.setupEndpoint(serverName, lsmEndpoint)
    rpcEnv.awaitTermination()

    memTableManagerThread.join()
    Thread.sleep(1000*1000)
  }

  startApplication()
}


class LSMServerEndpoint(override val rpcEnv: RpcEnv, val lsmServer : LSMServer) extends RpcEndpoint {
  override def onStart(): Unit = {
    println("start "+ epName +" endpoint")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case KVOp(key,value,op) => {
      op match {
        case "add" => {
          lsmServer.put(key, value)
          context.reply("ok")
        }
        case "query" => {
          val r = lsmServer.find(key)
          context.reply(r.apply(0).ts+":"+r.apply(0).value)
        }
      }
      context.reply("error")
    }
  }

  override def onStop(): Unit = {
    println("stop hello endpoint")
  }

  override val epName: String = ServerInstance.serverName
}


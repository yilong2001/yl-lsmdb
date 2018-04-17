package demo

import com.example.jrpc.nettyrpc.rpc.{HostPort, RpcConfig}
import com.example.srpc.nettyrpc.{RpcEndpointRef, RpcEnv}
import java.util
import java.util.Random
import java.util.concurrent.TimeUnit

import com.example.jutil.SSConfig
import com.example.lsm._
import com.example.srpc.nettyrpc.util.ThreadUtils
import demo.rpc.HelloClient.syncCall
import org.apache.http.HttpEntity
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{CloseableHttpResponse, HttpEntityEnclosingRequestBase, HttpGet, HttpPut}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.util.EntityUtils

import scala.concurrent.Await
import scala.language.implicitConversions
import scala.collection._
import scala.collection.mutable.ListBuffer

/**
  * Created by yilong on 2018/1/28.
  */


object ClientInstance {
  case class KVOp(key:String,value:String,op:String)

  val serverName = "lsm-server"

  def main(args: Array[String]): Unit = {
    syncCall()
  }

  def syncCall() = {
    val threadPool = ThreadUtils.newDaemonCachedThreadPool("pool", 2000)

    val rpcEnv: RpcEnv = RpcEnv.create(new RpcConfig(), "localhost", 9091, true)

    val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(new HostPort("localhost", 9091), serverName)

    (0 to 1000).foreach(id => {
      threadPool.submit(new Runnable {
        override def run(): Unit = {
          val result = endPointRef.askSync[String](KVOp(("key_"+id),("value_"+id),"add"), 1000*10)
          println(id + " : " + result)
        }
      })
    })
    //threadPool.wait(1000*10)

    (0 to 1000).foreach(id => {
      threadPool.submit(new Runnable {
        override def run(): Unit = {
          val result = endPointRef.askSync[String](KVOp(("key_"+id),null,"get"), 1000*10)
          println(id + " : " + result)
        }
      })
    })

    threadPool.awaitTermination(10000, TimeUnit.MILLISECONDS)
    threadPool.shutdownNow()
    println("********** end *************")
  }
}

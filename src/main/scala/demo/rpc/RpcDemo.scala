package demo.rpc


/**
  * Created by yilong on 2018/3/26.
  */

object RpcDemo {
  case class NNO(str : String)

  def main(args: Array[String]): Unit = {
    val x:String = "s1"
    val y:String = "s2"
    val c1 = NNO("c1");

    //val out = javaSerd.serialize(c1)
    //val t = javaSerd.deserialize(out)

    val serde = new com.example.srpc.nettyrpc.serde.RpcSerializer
    val out = serde.serialize[NNO](c1)//
    val t = serde.deserialize[NNO](out)
    println(t)
  }


}

package com.example.sutil

import com.example.jutil.Constant

import scala.collection._

/**
  * Created by yilong on 2018/1/25.
  */
class DataBlock(var firstKey : String) extends IBlock {
  //val buf = java.nio.ByteBuffer.allocate(Constant.BLOCK_SIZE)
  private val buf = new mutable.ListBuffer[Array[Byte]]
  private var cur = 0
  var lastKey = firstKey

  def save(data : Array[Byte]) : Boolean = {
    (Constant.BLOCK_SIZE >= data.length + cur) match {
      case false => false
      case true => {
        //System.arraycopy(data,0,buf,cur,data.length)
        //buf.put(data, cur, data.length)
        buf.append(data)
        cur += data.length
        true
      }
    }
  }

  def avaliableSize() : Int = {
    Constant.BLOCK_SIZE - cur
  }

  //def getBuf() : Array[Byte] = {
  //  buf.array()
  //}

  def updateLastKey(k:String) = {
    lastKey = k
  }

  def getLastKey() : String = {
    lastKey
  }

  def getBufArray() : mutable.ListBuffer[Array[Byte]] = {
    buf
  }

  def clean() = {
    buf.clear()
  }
}

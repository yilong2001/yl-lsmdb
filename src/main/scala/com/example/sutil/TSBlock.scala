package com.example.sutil

import com.example.jutil.Constant

import scala.collection._

/**
  * Created by yilong on 2018/1/25.
  */
class TSBlock(var firstTs : Int) extends IBlock {
  //val buf = java.nio.ByteBuffer.allocate(Constant.BLOCK_SIZE)
  private val buf = new mutable.ListBuffer[Array[Byte]]
  private var cur = 0
  var lastTs = firstTs

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

  def updateLastTs(ts:Int) = {
    lastTs = ts
  }

  def getLastTs() : Int = {
    lastTs
  }

  def getBufArray() : mutable.ListBuffer[Array[Byte]] = {
    buf
  }

  def clean() = {
    buf.clear()
  }
}

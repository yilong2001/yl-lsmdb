package com.example.sutil

import com.example.lsm.BufferOffset

/**
  * Created by yilong on 2018/2/24.
  */
object BufferUtils {
  def readBuffer(data : Array[Byte], curPos : Int, off: Int) : BufferOffset = {
    val newBuf = data.slice(curPos, curPos + off)
    val newPos = curPos + off
    BufferOffset(newBuf, newPos)
  }
}

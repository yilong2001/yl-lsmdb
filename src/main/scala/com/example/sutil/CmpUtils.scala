package com.example.sutil

import com.example.lsm.TimestampRange

/**
  * Created by yilong on 2018/2/27.
  */
object CmpUtils {
  def isTsSatisfied(ts:Long, range:TimestampRange) : Boolean = {
    (ts <= range.max) && (ts >= range.min)
  }
}

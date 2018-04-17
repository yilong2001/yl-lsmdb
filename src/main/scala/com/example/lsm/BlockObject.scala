package com.example.lsm

import scala.collection.immutable

/**
  * Created by yilong on 2018/2/22.
  */
case class DataBlockIndexOffset(firstKey:String, lastKey:String, offset:Int)
case class TSBlockIndexOffset(firstTs:Long, lastTs:Long, offset:Int)

case class BlockFoot(version:Int, indexBlocks:Int, tsBase: Long)

case class BufferOffset(buf:Array[Byte], off:Int)

case class DataFileMeta(filename:String, indexBlocks:Int, firstKey:String, lastKey:String, orgId: String)
case class TSFileMeta(filename:String, indexBlocks:Int, firstTs:Long, lastTs:Long, orgId: String)

case class TSFileCacheData(indexBlocks: List[TSBlockIndexOffset], foot: BlockFoot)

case class IndexOffsetTimeout(indexOffset: immutable.Map[String,Int], timeout: Long)

case class TimestampValue(ts:Long, value:String)

case class TimestampRange(min:Long, max:Long)

package com.example.lsm

import java.io.{File, FileOutputStream}
import java.nio.charset.Charset

import com.example.jutil.{Constant, SSConfig}
import com.example.sutil.{LSMFileUtils, TSBlock}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

/**
  * Created by yilong on 2018/1/25.
  */
class LSMTSFileFlusher(val config : SSConfig) {
  case class TimestampKey(ts:Long, key:String)

  def updateBlocks(tsdeta : Int, curBlock : TSBlock, blocks : ArrayBuffer[TSBlock], byteSize : Int) : TSBlock = {
    var block = curBlock
    if (block == null) {
      block = new TSBlock(tsdeta)
    }

    if (block.avaliableSize() < byteSize) {
      if (block.avaliableSize() == Constant.BLOCK_SIZE) {
        throw new RuntimeException("Constant.BLOCK_SIZE = " + Constant.BLOCK_SIZE + " is too small")
      }

      println("updateBlocks : blocks add ")
      blocks.append(block)
      block = new TSBlock(tsdeta)
    }

    println("updateBlocks over : blocks size = "+blocks.size)
    block
  }

  def saveTS(tsKeySortedList: List[TimestampKey], blocks : ArrayBuffer[TSBlock]) : TSBlock = {
    //all saved byte array must not be reused, because block only keeps array link
    //[key len], [v len], [key], [v], [timestamp]
    var curBlock : TSBlock = null

    println("*************************** SSTSFileFlusher.saveTS ***************************")
    val basets = tsKeySortedList.apply(0).ts

    tsKeySortedList.foreach( tskey => {
      val tsdetabuf = java.nio.ByteBuffer.allocate(Constant.KV_LEN_SIZE)
      val tsdeta = (tskey.ts - basets).asInstanceOf[Int]
      tsdetabuf.putInt(tsdeta)

      val keybts = tskey.key.getBytes(Charset.forName("UTF-8"))

      val keylenbuf = java.nio.ByteBuffer.allocate(Constant.KV_LEN_SIZE)
      keylenbuf.putInt(keybts.length)

      val totallen = tsdetabuf.array().length + keybts.length + keylenbuf.array().length
      curBlock = updateBlocks(tsdeta, curBlock, blocks, totallen)

      if (!curBlock.save(tsdetabuf.array())) {
        throw new RuntimeException("TSFlusher saveTS, curBlock save tsdetabuf failed! ")
      }
      if (!curBlock.save(keylenbuf.array())) {
        throw new RuntimeException("TSFlusher saveTS, curBlock save vlenbuf failed! ")
      }
      if (!curBlock.save(keybts)) {
        throw new RuntimeException("TSFlusher saveTS, curBlock save kbts failed! ")
      }

      curBlock.updateLastTs(tsdeta)
    })

    println("TSFlusher saveTS : blocks size = "+blocks.size)
    curBlock
  }

  def saveIndex(blocks : ArrayBuffer[TSBlock]) : TSBlock = {
    //all saved byte array must not be reused, because block only keeps array link
    var block : TSBlock = null
    var offset = 0

    blocks.foreach(bk => {
      var indexlen = Constant.KV_LEN_SIZE + Constant.KV_LEN_SIZE + Constant.KV_LEN_SIZE

      block = updateBlocks(0, block, blocks, indexlen)

      val ftsbuf = java.nio.ByteBuffer.allocate(Constant.KV_LEN_SIZE)
      ftsbuf.putInt(bk.firstTs)

      val ltsbuf = java.nio.ByteBuffer.allocate(Constant.KV_LEN_SIZE)
      ltsbuf.putInt(bk.lastTs)

      //[first key lenth, last key length, key first, , key last, offset]
      block.save(ftsbuf.array())
      block.save(ltsbuf.array())

      val offbuf = java.nio.ByteBuffer.allocate(Constant.KV_LEN_SIZE)
      offbuf.putInt(offset)
      block.save(offbuf.array())

      offset = offset + 1
    })

    block
  }

  def saveFoot(tsbase: Long, indexBlocks : Int) : TSBlock = {
    val block = new TSBlock(0)
    LSMFileUtils.saveFoot(config, tsbase, indexBlocks, block).asInstanceOf[TSBlock]
  }

  def saveIntoFile(filename : String, blocks : ArrayBuffer[TSBlock]) = {
    //JFileFlusher.saveIntoFile2(filename, blocks);
    //better style: save .sstx.tmp and then change to .sstx
    val f = new File(filename)
    if (!f.getParentFile.exists()) {
      f.getParentFile.mkdirs
    }
    f.createNewFile()

    val writer = new FileOutputStream(filename)

    try {
      blocks.foreach(bk => {
        bk.getBufArray().foreach(btarr => {
          writer.write(btarr)
        })
        if (bk.avaliableSize() > 0) {
          //padding empty byte for one block
          val tmp = java.nio.ByteBuffer.allocate(bk.avaliableSize())
          writer.write(tmp.array())
        }
      })
    } finally {
      //writer.flush()
      writer.getFD.sync()
      writer.close()
    }
    println("MemTableFlusher, start saveIntoFile -> " + filename)
  }

  def cleanBlocks(blocks : ArrayBuffer[TSBlock]) = {
    blocks.foreach(bk=>{bk.clean()})
    blocks.clear()
  }

  def flush(tsfilename : String, memTable: MemTable) : TSFileMeta = {
    println("MemTableFlusher, start flush, memtable size = " + memTable.getMemtable().size())
    var curBlock : TSBlock = null

    var blocks = new ArrayBuffer[TSBlock]

    val tsKeySortedList = memTable.getMemtable().entrySet().map(entry => {
      TimestampKey(entry.getValue.ts, entry.getKey)
    }).toList.sortWith((first, second) => {
      (first.ts < second.ts)
    })

    //save ts,key elements
    curBlock = saveTS(tsKeySortedList, blocks)
    blocks.append(curBlock)

    val firstTs = tsKeySortedList.apply(0).ts
    val lastTs = tsKeySortedList.apply(tsKeySortedList.size - 1).ts

    val blockssize1 = blocks.size

    //save index
    curBlock = saveIndex(blocks)
    blocks.append(curBlock)

    val indexBlocks = blocks.size - blockssize1

    //save foot
    curBlock = saveFoot(firstTs, indexBlocks)
    blocks.append(curBlock)

    //save into file
    saveIntoFile(tsfilename, blocks)

    //clean blocks
    cleanBlocks(blocks)

    TSFileMeta(tsfilename, indexBlocks, firstTs, lastTs, memTable.getId())
  }
}

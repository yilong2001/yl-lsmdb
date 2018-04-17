package com.example.lsm

import java.io.{File, FileOutputStream, FileWriter, PrintWriter}
import java.nio.charset.Charset

import com.example.jutil.{Constant, JFileFlusher, SSConfig}
import com.example.sutil.{DataBlock, LSMFileUtils}
//import com.example.jutils.DataBlock

import scala.collection.mutable.ArrayBuffer
//import scala.collection.JavaConversions._

/**
  * Created by yilong on 2018/1/25.
  */
class LSMDataFileFlusher(val config : SSConfig) {

  def updateBlocks(k : String, curBlock : DataBlock, blocks : ArrayBuffer[DataBlock], byteSize : Int) : DataBlock = {
    println("updateBlocks start : blocks size = "+blocks.size+"; byteSize = " + byteSize)
    var block = curBlock
    if (block == null) {
      block = new DataBlock(k)
    }

    if (block.avaliableSize() < byteSize) {
      if (block.avaliableSize() == Constant.BLOCK_SIZE) {
        throw new RuntimeException("Constant.BLOCK_SIZE = " + Constant.BLOCK_SIZE + " is too small")
      }

      println("updateBlocks : blocks add ")
      blocks.append(block)
      block = new DataBlock(k)
    }

    println("updateBlocks over : blocks size = "+blocks.size)
    block
  }

  def saveKV(memTable: MemTable, blocks : ArrayBuffer[DataBlock]) : DataBlock = {
    //all saved byte array must not be reused, because block only keeps array link
    //[key len], [v len], [key], [v], [timestamp]
    var curBlock : DataBlock = null
    val curMemTable = memTable.getMemtable()

    println("*************************** SSFileFlusher.saveKV ***************************")

    println("SSFileFlusher.saveKV : mt.size=" + memTable.getMemtable().size())

    val sortedMap = new java.util.TreeMap[String, TimestampValue]()
    val it1 = curMemTable.entrySet().iterator()
    while(it1.hasNext) {
      val entry = it1.next()
      sortedMap.put(entry.getKey, entry.getValue)
    }

    val it = sortedMap.entrySet().iterator()
    while(it.hasNext) {
      val entry = it.next()
      val kbts = entry.getKey.getBytes(Charset.forName("UTF-8"))
      val vbts = entry.getValue.value.getBytes(Charset.forName("UTF-8"))

      val klenbuf = java.nio.ByteBuffer.allocate(Constant.KV_LEN_SIZE)
      klenbuf.putInt(kbts.length)

      val vlenbuf = java.nio.ByteBuffer.allocate(Constant.KV_LEN_SIZE)
      vlenbuf.putInt(vbts.length)

      val tsbuf = java.nio.ByteBuffer.allocate(Constant.LONG_SIZE)
      tsbuf.putLong(entry.getValue.ts)

      val totallen = klenbuf.array().length + vlenbuf.array().length + kbts.length + vbts.length + tsbuf.array().length
      curBlock = updateBlocks(entry.getKey, curBlock, blocks, totallen)

      if (!curBlock.save(klenbuf.array())) {
        throw new RuntimeException("MemTableFlusher saveKV, curBlock save klenbuf failed! ")
      }
      if (!curBlock.save(vlenbuf.array())) {
        throw new RuntimeException("MemTableFlusher saveKV, curBlock save vlenbuf failed! ")
      }
      if (!curBlock.save(kbts)) {
        throw new RuntimeException("MemTableFlusher saveKV, curBlock save kbts failed! ")
      }
      if (!curBlock.save(vbts)) {
        throw new RuntimeException("MemTableFlusher saveKV, curBlock save vbts failed! ")
      }
      if (!curBlock.save(tsbuf.array())) {
        throw new RuntimeException("MemTableFlusher saveKV, curBlock save vbts failed! ")
      }

      curBlock.updateLastKey(entry.getKey)
    }

    println("SSFileFlusher.saveKV : memtable blocks size = "+blocks.size)
    curBlock
  }

  def saveIndex(blocks : ArrayBuffer[DataBlock]) : DataBlock = {
    //all saved byte array must not be reused, because block only keeps array link
    var block : DataBlock = null
    var offset = 0

    blocks.foreach(bk => {
      val firstkeybts = bk.firstKey.getBytes(Charset.forName("UTF-8"))
      val lastkeybts = bk.lastKey.getBytes(Charset.forName("UTF-8"))
      //println("saveIndex bk.firstKey = "+bk.firstKey+":" + firstkeybts.length+", lastKey = "+bk.lastKey+":" + lastkeybts.length)

      var indexlen = Constant.KV_LEN_SIZE + firstkeybts.length
      indexlen = indexlen + Constant.KV_LEN_SIZE + lastkeybts.length
      indexlen = indexlen + Constant.KV_LEN_SIZE

      block = updateBlocks("", block, blocks, indexlen)

      val fklenbuf = java.nio.ByteBuffer.allocate(Constant.KV_LEN_SIZE)
      fklenbuf.putInt(firstkeybts.length)

      val lklenbuf = java.nio.ByteBuffer.allocate(Constant.KV_LEN_SIZE)
      lklenbuf.putInt(lastkeybts.length)

      //[first key lenth, last key length, key first, , key last, offset]
      block.save(fklenbuf.array())
      block.save(lklenbuf.array())

      block.save(firstkeybts)
      block.save(lastkeybts)

      val offbuf = java.nio.ByteBuffer.allocate(Constant.KV_LEN_SIZE)
      offbuf.putInt(offset)
      block.save(offbuf.array())

      offset = offset + 1
    })

    block
  }

  def saveFoot(indexBlocks : Int) : DataBlock = {
    val block = new DataBlock(null)
    LSMFileUtils.saveFoot(config, System.currentTimeMillis(), indexBlocks, block).asInstanceOf[DataBlock]
  }

  def saveIntoFile(filename : String, blocks : ArrayBuffer[DataBlock]) = {
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

  def cleanBlocks(blocks : ArrayBuffer[DataBlock]) = {
    blocks.foreach(bk=>{bk.clean()})
    blocks.clear()
  }

  def flush(datafilename : String, memTable: MemTable) : DataFileMeta = {
    println("MemTableFlusher, start flush, memtable size = " + memTable.getMemtable().size())
    var curBlock : DataBlock = null

    var blocks = new ArrayBuffer[DataBlock]

    //save k,v elements
    curBlock = saveKV(memTable, blocks)
    blocks.append(curBlock)

    val firstkey = blocks.apply(0).firstKey
    val lastkey = blocks.apply(blocks.size - 1).getLastKey()

    val blockssize1 = blocks.size

    //save index
    curBlock = saveIndex(blocks)
    blocks.append(curBlock)

    val indexBlocks = blocks.size - blockssize1

    //save foot
    curBlock = saveFoot(indexBlocks)
    blocks.append(curBlock)

    //save into file
    saveIntoFile(datafilename, blocks)

    //clean blocks
    cleanBlocks(blocks)

    DataFileMeta(datafilename, indexBlocks, firstkey, lastkey, memTable.getId())
  }
}

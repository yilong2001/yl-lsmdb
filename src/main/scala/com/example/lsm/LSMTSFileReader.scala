package com.example.lsm

import java.io.{File, FileInputStream}
import java.nio.charset.Charset

import com.example.jutil.{Constant, SSConfig}
import com.example.sutil.{BufferUtils, LSMFileUtils}

import scala.collection.{immutable, mutable}

/**
  * Created by yilong on 2018/1/26.
  * 基于时间查询，在本地缓存上次查询过的查询条件+查询结果，以便于下次再查（例如，增量/分页查询）
  */
class LSMTSFileReader(config:SSConfig) {
  def byteArrayToInt(b: Array[Byte]): Int = b(3) & 0xFF | (b(2) & 0xFF) << 8 | (b(1) & 0xFF) << 16 | (b(0) & 0xFF) << 24

  def readIndexBlocks(filename : String, foot : BlockFoot) : immutable.List[TSBlockIndexOffset] = {
    val blockIndexOffsets = new mutable.ListBuffer[TSBlockIndexOffset]()

    val inputFile = new File(filename)
    val data = new Array[Byte](Constant.BLOCK_SIZE)

    var fis : FileInputStream = null
    try {
      fis = new FileInputStream(inputFile)
      var off = config.getFirstIndexBlockOffset(inputFile.length().asInstanceOf[Int], foot.indexBlocks)

      println(filename + " length = " + inputFile.length() + "; indexBlocks = " + foot.indexBlocks + ";  indexOff = " + off)
      fis.skip(off)

      for (i <- 0 to (foot.indexBlocks - 1)) {
        for (j <- 0 to (Constant.BLOCK_SIZE - 1)) {
          data(j) = 0
        }

        fis.read(data, 0, Constant.BLOCK_SIZE)

        var bufOff : BufferOffset = BufferOffset(null, 0)

        while (bufOff.off < (Constant.BLOCK_SIZE - 3 * Constant.KV_LEN_SIZE)) {
          //first TS, last TS, offset
          bufOff = BufferUtils.readBuffer(data, bufOff.off, Constant.KV_LEN_SIZE)
          val firstTsDeta = byteArrayToInt(bufOff.buf)

          bufOff = BufferUtils.readBuffer(data, bufOff.off, Constant.KV_LEN_SIZE)
          val lastTsDeta = byteArrayToInt(bufOff.buf)

          bufOff = BufferUtils.readBuffer(data, bufOff.off, Constant.KV_LEN_SIZE)
          val blockOff = byteArrayToInt(bufOff.buf)

          if (firstTsDeta < 0 || lastTsDeta < 0) {
            //
          } else {
            if (lastTsDeta > 0) {
              blockIndexOffsets.append(TSBlockIndexOffset(foot.tsBase + firstTsDeta, foot.tsBase + lastTsDeta, blockOff))
            }
          }
        }
      }
    }catch {
      case e:Exception => e.printStackTrace()
    } finally {
      if (fis != null) {
        fis.close()
      }
    }

    blockIndexOffsets.toList
  }

  def isTsSatisfied(ts:Long, range:TimestampRange) : Boolean = {
    (ts <= range.max) && (ts >= range.min)
  }

  def readDataBlock(range: TimestampRange, filename:String, offset:Int, foot: BlockFoot) : immutable.List[String] = {
    val keylist = new mutable.ListBuffer[String]()

    val inputFile = new File(filename)
    val data = new Array[Byte](Constant.BLOCK_SIZE)
    try {
      var fis = new FileInputStream(inputFile)
      println("SSFileReader.readBlock : "+filename + " length = " + inputFile.length() + ";  off = " + offset)
      fis.skip(offset * Constant.BLOCK_SIZE)
      fis.read(data, 0, Constant.BLOCK_SIZE)

      var bufOff : BufferOffset = BufferOffset(null, 0)
      var keylen = 1
      var tsDeta = 0
      while (keylen > 0 && bufOff.off <= (Constant.BLOCK_SIZE - (2 * Constant.KV_LEN_SIZE))) {
        //step 1, ts deta [4byte]
        bufOff = BufferUtils.readBuffer(data, bufOff.off, Constant.KV_LEN_SIZE)
        tsDeta = byteArrayToInt(bufOff.buf)

        //step 2, key len [4byte]
        bufOff = BufferUtils.readBuffer(data, bufOff.off, Constant.KV_LEN_SIZE)
        keylen = byteArrayToInt(bufOff.buf)

        if (keylen <= 0) {
          //
        } else {
          //step 3, the value of key[key len]
          bufOff = BufferUtils.readBuffer(data, bufOff.off, keylen)
          val key = new String(bufOff.buf, Charset.forName("UTF-8"))

          if (isTsSatisfied(tsDeta+foot.tsBase, range)) {
            keylist.append(key)
          }
        }
      }

      fis.close()
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {

    }

    keylist.toList
  }

  def find(range: TimestampRange, filename:String): immutable.List[String] = {
    val keylist = new mutable.ListBuffer[String]()

    val foot = LSMFileUtils.readFoot(filename, config)

    readIndexBlocks(filename, foot).filter(tsblock => {
      tsblock.firstTs <= range.max && tsblock.lastTs >= range.min
    }).foreach(tsblock => {
      keylist.++=:(readDataBlock(range, filename, tsblock.offset, foot))
    })

    keylist.toList
  }

  def find(range: TimestampRange, filename:String, cacheData : TSFileCacheData): List[String] = {
    val keylist = new mutable.ListBuffer[String]()

    cacheData.indexBlocks.filter(tsblock => {
      tsblock.firstTs <= range.max && tsblock.lastTs >= range.min
    }).foreach(tsblock => {
      keylist ++ readDataBlock(range, filename, tsblock.offset, cacheData.foot)
    })

    keylist.toList
  }

}

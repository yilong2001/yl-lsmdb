package com.example.lsm

import scala.collection.mutable
import scala.collection.immutable
import java.io.{File, FileInputStream}
import java.nio.charset.Charset
import java.util

import com.example.jutil.{Constant, SSConfig}
import com.example.sutil.{BufferUtils, LSMFileUtils}

/**
  * Created by yilong on 2018/1/26.
  *
  */
class LSMDataFileReader(config:SSConfig) {
  def byteArrayToInt(b: Array[Byte]): Int = b(3) & 0xFF | (b(2) & 0xFF) << 8 | (b(1) & 0xFF) << 16 | (b(0) & 0xFF) << 24

  def readIndexBlocks(filename : String) : immutable.List[DataBlockIndexOffset] = {
    val blockIndexOffsets = new mutable.ListBuffer[DataBlockIndexOffset]()

    val inputFile = new File(filename)
    val data = new Array[Byte](Constant.BLOCK_SIZE)

    val foot = LSMFileUtils.readFoot(filename, config)

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

        var firstKeylen = 1
        var lastKeylen = 1
        var bufOff : BufferOffset = BufferOffset(null, 0)

        while (firstKeylen > 0 && lastKeylen > 0 && bufOff.off < (Constant.BLOCK_SIZE - 2*Constant.KV_LEN_SIZE)) {
          //step 1, fist key len[4byte], last key len[4byte]
          bufOff = BufferUtils.readBuffer(data, bufOff.off, Constant.KV_LEN_SIZE)
          firstKeylen = byteArrayToInt(bufOff.buf)

          bufOff = BufferUtils.readBuffer(data, bufOff.off, Constant.KV_LEN_SIZE)
          lastKeylen = byteArrayToInt(bufOff.buf)

          if (firstKeylen <= 0 || lastKeylen <= 0) {
            //
          } else {
            //step 2, the value of fist key[first key len], the value of last key[last key len],
            bufOff = BufferUtils.readBuffer(data, bufOff.off, firstKeylen)
            val firstKey = new String(bufOff.buf, Charset.forName("UTF-8"))

            bufOff = BufferUtils.readBuffer(data, bufOff.off, lastKeylen)
            val lastKey = new String(bufOff.buf, Charset.forName("UTF-8"))

            bufOff = BufferUtils.readBuffer(data, bufOff.off, Constant.KV_LEN_SIZE)
            val keyoffset = byteArrayToInt(bufOff.buf)

            blockIndexOffsets.append(DataBlockIndexOffset(firstKey, lastKey, keyoffset))
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

  def readDataBlock(filename:String, offset:Int) : immutable.TreeMap[String, TimestampValue] = {
    val kvMap = new mutable.HashMap[String, TimestampValue]()

    val inputFile = new File(filename)
    val data = new Array[Byte](Constant.BLOCK_SIZE)
    try {
      var fis = new FileInputStream(inputFile)
      println("SSFileReader.readBlock : "+filename + " length = " + inputFile.length() + ";  off = " + offset)
      fis.skip(offset * Constant.BLOCK_SIZE)
      fis.read(data, 0, Constant.BLOCK_SIZE)

      var keylen = 1
      var vlen = 0
      var bufOff : BufferOffset = BufferOffset(null, 0)

      while (keylen > 0 && bufOff.off <= (Constant.BLOCK_SIZE - (2 * Constant.KV_LEN_SIZE))) {
        //step 1, key len [4byte]
        bufOff = BufferUtils.readBuffer(data, bufOff.off, Constant.KV_LEN_SIZE)
        keylen = byteArrayToInt(bufOff.buf)
        //step 2, value len [4byte]
        bufOff = BufferUtils.readBuffer(data, bufOff.off, Constant.KV_LEN_SIZE)
        vlen = byteArrayToInt(bufOff.buf)

        if (keylen <= 0 || vlen <= 0) {
          //
        } else {
          //step 3, the value of key[key len]
          bufOff = BufferUtils.readBuffer(data, bufOff.off, keylen)
          val key = new String(bufOff.buf, Charset.forName("UTF-8"))
          //step 4, the value of value[value len]
          bufOff = BufferUtils.readBuffer(data, bufOff.off, vlen)
          val value = new String(bufOff.buf, Charset.forName("UTF-8"))

          bufOff = BufferUtils.readBuffer(data, bufOff.off, Constant.LONG_SIZE)
          val ts = java.nio.ByteBuffer.wrap(bufOff.buf).getLong()

          kvMap.put(key, TimestampValue(ts, value))
        }
      }

      fis.close()
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {

    }

    val x = new immutable.TreeMap[String, TimestampValue]
    x ++ kvMap
  }



}

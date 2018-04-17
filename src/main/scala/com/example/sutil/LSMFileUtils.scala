package com.example.sutil

import java.io.{File, FileInputStream}

import com.example.jutil.{Constant, SSConfig}
import com.example.lsm.BlockFoot

/**
  * Created by yilong on 2018/2/25.
  */
object LSMFileUtils {
  def byteArrayToInt(b: Array[Byte]): Int = b(3) & 0xFF | (b(2) & 0xFF) << 8 | (b(1) & 0xFF) << 16 | (b(0) & 0xFF) << 24

  def readFoot(filename:String, config : SSConfig) : BlockFoot = {
    val inputFile = new File(filename)
    val data = new Array[Byte](Constant.BLOCK_SIZE)

    var fis : FileInputStream = null
    var ver : Int = 0
    var baseTs: Long = 0
    var indexBlocks : Int = 0

    try {
      fis = new FileInputStream(inputFile)
      fis.skip(config.getFootBlockOffset(inputFile.length().asInstanceOf[Int]))

      fis.read(data, 0, Constant.BLOCK_SIZE)

      ver = byteArrayToInt(data.slice(config.getVersionOffsetInFoot(),
        config.getVersionOffsetInFoot()+Constant.KV_LEN_SIZE))

      baseTs = java.nio.ByteBuffer.wrap(data.slice(config.getBaseTsOffsetInFoot(),
        config.getBaseTsOffsetInFoot()+Constant.LONG_SIZE)).getLong

      indexBlocks = byteArrayToInt(data.slice(config.getIndexBlocksOffsetInFoot(),
        config.getIndexBlocksOffsetInFoot()+Constant.KV_LEN_SIZE))
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if(fis != null) { fis.close() }
    }

    BlockFoot(ver, indexBlocks, baseTs)
  }

  def saveFoot (config: SSConfig, tsbase: Long, indexBlocks : Int, block : IBlock) : IBlock = {
    //version
    val verbuf = java.nio.ByteBuffer.allocate(Constant.KV_LEN_SIZE)
    verbuf.putInt(config.getVersion)
    block.save(verbuf.array())

    //ts base
    val tsbasebuf = java.nio.ByteBuffer.allocate(Constant.LONG_SIZE)
    tsbasebuf.putLong(tsbase)
    block.save(tsbasebuf.array())

    //index blocks
    val klenbuf = java.nio.ByteBuffer.allocate(Constant.KV_LEN_SIZE)
    klenbuf.putInt(indexBlocks)
    block.save(klenbuf.array())

    //magic number
    import java.security.MessageDigest
    val md5 = MessageDigest.getInstance("MD5")

    val magicbuf = java.nio.ByteBuffer.allocate(32)
    val digest = md5.digest(magicbuf.array())
    block.save(digest.take(2))

    block
  }
}

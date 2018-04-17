package com.example.jutil;

/**
 * Created by yilong on 2018/1/25.
 */
public class SSConfig {
    final String base = "/Users/yilong/work/bigdata/code/stream/lsmdemo/debug";

    public SSConfig() {
    }

    public String getRootDir(int level) {
        return "/Users/yilong/work/bigdata/code/stream/lsmdemo/debug/level_" + level;
    }

    public String getTableName() {
        return "default";
    }

    public String getLevel0DataFilename(long ts) {
        String level0RootDir = this.getRootDir(0);
        String filename = level0RootDir + "/" + this.getTableName() + "/" + ts + ".sstd";
        return filename;
    }

    public String getLevel0TSFilename(long ts) {
        String level0RootDir = this.getRootDir(0);
        String filename = level0RootDir + "/" + this.getTableName() + "/" + ts + ".sstt";
        return filename;
    }

    public int getVersion() {
        return 1;
    }

    public int getFootBlockOffset(int fileLen) {
        return fileLen - 128;
    }

    public int getFirstIndexBlockOffset(int fileLen, int indexBlocks) {
        return fileLen - 128 * (indexBlocks + 1);
    }

    public int getVersionOffsetInDataFileFoot() {
        return 0;
    }

    public int getIndexBlocksOffsetInDataFileFoot() {
        return 4;
    }

    public int getVersionOffsetInFoot() {
        return 0;
    }

    public int getBaseTsOffsetInFoot() {
        return 4;
    }

    public int getIndexBlocksOffsetInFoot() {
        return 12;
    }

//    final String base = "/Users/yilong/work/bigdata/code/stream/lsmdemo/debug";
//    public String getRootDir(int level) {
//        return base + "/level_" + level;
//    }
//
//    public String getTableName() {
//        return "default";
//    }
//
//    public String getLevel0DataFilename(long ts) {
//        String level0RootDir = getRootDir(Constant.LEVEL_0);
//        String filename = level0RootDir + "/" + getTableName() + "/" + ts + ".sstd";
//        return filename;
//    }
//
//    public String getLevel0TSFilename(long ts) {
//        String level0RootDir = getRootDir(Constant.LEVEL_0);
//        String filename = level0RootDir + "/" + getTableName() + "/" + ts + ".sstt";
//        return filename;
//    }
//
//    public int getVersion() {
//        return 1;
//    }
//
//    public int getFootBlockOffset(int fileLen) {
//        return fileLen - Constant.BLOCK_SIZE;
//    }
//    public int getFirstIndexBlockOffset(int fileLen, int indexBlocks) {
//        return fileLen - Constant.BLOCK_SIZE * (indexBlocks + 1);
//    }
//
//    public int getVersionOffsetInDataFileFoot() {
//        return 0;
//    }
//    public int getIndexBlocksOffsetInDataFileFoot() {
//        return Constant.KV_LEN_SIZE;
//    }
//
//
//    public int getVersionOffsetInTSFileFoot() {
//        return 0;
//    }
//    public int getBaseTsOffsetInTSFileFoot() {
//        return Constant.KV_LEN_SIZE;
//    }
//    public int getIndexBlocksOffsetInTSFileFoot() {
//        return Constant.KV_LEN_SIZE + Constant.LONG_SIZE;
//    }
}

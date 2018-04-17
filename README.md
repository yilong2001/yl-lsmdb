# yl-lsmdb
参考hbase的实现原理，实现一种lsm db。

主要考虑的特性：
- 基于netty的rpc框架(参考 yl-netty-rpc)
- 同时支持时间索引(hbase仅支持rowkey索引)
- 采用固定内存分配以减少GC(参考storm disruptor的实现机制)


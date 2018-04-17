# yl-lsmdb
参考hbase(以及 leveldb 等)实现原理，实现一种lsm db。主要特性：基于netty的rpc框架(参考 yl-netty-rpc)、同时支持时间索引、采用固定内存分配以减少GC(参考storm disruptor、spark offheap的内存管理方法)

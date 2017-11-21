# goproducer
- 一个基于go-mysql &amp;gorail二次开发的binlog解析工具，最终生成SQL语句使用MQ发送到消费者进行执行（这里使用banyandb的Queue可更换kafka）
- 增加了修改表结构（alter table）后依然能正常工作的特性
- 多个mysql同时处理的功能
- 解决跨机房mysql自带同步缓慢的问题
- * 修改了go-mysql源码所以必须使用vendor下的包

## build
- make
- ./binlog_producer

## config
- 在conf目录下修改 producer.toml 配置mysql地址，banyandb集群agent地址和库表，mysql主库可以添加多个同时处理，只要保证id 、data_path和queue选项不重复即可
- 程序运行后会在conf目录下生成data_path配置的master.info文件，保存binlog文件名 pos号信息 
- * 如果同步遇到drop table、drop database命令或者程序异常中断之后有表结构更改（alter table）命令那么需要删除master.info文件并手动进行全量同步，否则可能出现解析表字段名异常

# 已退役的共享持久化模块

保留上一版源码与注释用于学习对照。本模块已移出 microservices-server 的 Maven modules，当前四个业务服务均不依赖它。不要将这里的 Entity、Repository 或数据库连接配置重新引入微服务；每个服务的数据访问代码位于其自己的模块中。

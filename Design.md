# 架构说明

+ 总体架构

<img src="C:\Users\squarehuang\AppData\Roaming\Typora\typora-user-images\image-20240506130419482.png" alt="image-20240506130419482"  />

+ MasterServer:
  + Client的通信入口，负责转发请求
    + 如何转发请求：根据Meta信息返回对应的服务器地址
  + 保存Region的Metadata：一个ArrayList，包括该RegionMeta存储的所有table，Region的slave数量

![image-20240506131727347](C:\Users\squarehuang\AppData\Roaming\Typora\typora-user-images\image-20240506131727347.png)



+ RegionServer:
  + 与数据库进行交互
  + 主从复制，异步复制

![image-20240506131706656](C:\Users\squarehuang\AppData\Roaming\Typora\typora-user-images\image-20240506131706656.png)

+ Zookeeper:
  + zookeeper干了什么：个人理解是保证了zookeeper服务器上数据的一致性，也就是每个server看到的region下的任何数据都是一样的

![image-20240506131559208](C:\Users\squarehuang\AppData\Roaming\Typora\typora-user-images\image-20240506131559208.png)
# 项目说明

本项目是一个基于Mysql的简易分布式 MiniSQL 系统，是浙江大学2023-2024《大规模信息系统构建技术导论》的课程项目。本项目包含 Client，Master Server，Region Server 和 Zookeeper 共四个模块，实现一个在多主机上共享数据资源访问的分布式数据库系统，还实现了前端网页界面，能够对简单 SQL 语句进行处理和解析，实现了基本分布式数据库的功能，包括容错容灾、负载均衡、副本管理等。  

本项目的 Master Server、Region Server、Zookeeper 使用 Java 语言开发，使用 Maven和Gradle 作为项目管理工具，Client 基于 React框架进行开发。整体项目并使用 Github 进行版本管理和协作开发，由小组内的三名成员共同完成。
# FlinkTutorial
Flink from zero to one <br>

[WordCount](/src/main/jave/com/jw9j/wc/WordCount.java): 基于DataSet的wordcount
<br>[StreamWordCount](/src/main/jave/com/jw9j/wc/StreamWordCount.java) : 流数据读取处理
<br>

## 三. flink Source 支持与简单实现
1. [Collection](src/main/java/com/jw9j/source/SourceTest3_Collection.java) 集合
2. [文件读取](src/main/java/com/jw9j/source/SourceTest3_File.java)
3. [Kafka](src/main/java/com/jw9j/source/SourceTest3_Kafka.java)等消息队列
4. [UDF](src/main/java/com/jw9j/source/SourceTest4_UDF.java)( 函数类,匿名函数类,富函数)
   > 富函数
   > 富函数可以获取运行环境上下文,并拥有一些生命周期方法,所以可以实现更复杂的功能.
## 四. flink中的Transform

## 五. flink支持的数据类型
1. 基础数据类型
2. Java和Scala元组
3. Scala样例类
4. Java简单对象(POJOS)

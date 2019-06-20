package com.atguigu.dw.gmall0105.common.util
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}

/**
  * Author lzc
  * Date 2019-06-17 19:07
  */
object MyESUtil {
    
    private val ES_HOST = "http://hadoop201"
    private val ES_HTTP_PORT = 9200
    
    private val factory = buildFactory
    
    /**
      * 构建客户端工厂对象
      *
      */
    def buildFactory = {
        val config: HttpClientConfig = new HttpClientConfig.Builder(s"$ES_HOST:$ES_HTTP_PORT")
            
            .multiThreaded(true)
            .maxTotalConnection(20)
            .connTimeout(10000)
            .readTimeout(10000)
            .build()
        val factory = new JestClientFactory
        factory.setHttpClientConfig(config)
        factory
    }
    
    /**
      * 获取客户端对象
      *
      * @return
      */
    def getClient = {
        factory.getObject
    }
    
    def closeClient(client: JestClient) = {
        if (client != null) {
            try {
                client.shutdownClient()
            } catch {
                case e => e.printStackTrace()
            }
        }
    }
    
    /**
      * 插入
      * @param index
      * @param source
      */
    def insertBulk(index: String, source: Iterable[Any]) ={
        val bulkBuilder: Bulk.Builder = new Bulk.Builder().defaultIndex(index).defaultType("_doc")
        source.foreach(any => {
            bulkBuilder.addAction(new Index.Builder(any).build())
        })
        val client: JestClient = getClient
        client.execute(bulkBuilder.build)
        closeClient(client)
        
    }
    def main(args: Array[String]): Unit = {
    
//        singleOperation()
        multiOperation()
        
    }
    
    /**
      * 单次操作测试
      */
    def singleOperation() = {
        val client: JestClient = getClient
        // 1. 插入json格式数据
        val source1 =
            """
              |{
              |  "mid": "mid_123456789",
              |  "uid": "1234"
              |}
            """.stripMargin
        val index1: Index = new Index.Builder(source1)
            .index("gmall_dau")
            .`type`("_doc")
            .build()
        client.execute(index1)
        
        
        // 2. 插入样例类
        
        val source2 = User("lisi", "1234")
        val index2: Index = new Index.Builder(source2)
            .index("gmall_dau")
            .`type`("_doc")
            .build()
        client.execute(index2)
        
        
        closeClient(client)
    }
    
    /**
      * 批量操作测试
      */
    def multiOperation() = {
        val client: JestClient = getClient
        val source1 = User("lisi1", "a")
        val source2 = User("lisi2", "b")
        
        val bulk: Bulk = new Bulk.Builder()
            .defaultIndex("gmall_dau")
            .defaultType("_doc")
            .addAction(new Index.Builder(source1).build())
            .addAction(new Index.Builder(source2).build())
            .build()
        
        client.execute(bulk)
        
        closeClient(client)
    }
    
}


case class User(mid: String, uid: String)
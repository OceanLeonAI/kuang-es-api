package com.kuang;

import com.alibaba.fastjson.JSON;
import com.kuang.pojo.User;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import javax.naming.directory.SearchResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Elasticsearch 7.6x 客户端测试 API
 */
@SpringBootTest
class KuangshenEsApiApplicationTests {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    /**
     * 测试创建索引
     *
     * @throws IOException
     */
    @Test
    void testCreateIndex() throws IOException {
        //1.创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("index_leon");
        //2.客户端执行请求 CreateIndexRequest，返回响应
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }

    /**
     * 测试索引是否存在
     *
     * @throws IOException
     */
    @Test
    void testExistIndex() throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest("index_leon");
        boolean exists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     * 测试删除索引
     *
     * @throws IOException
     */
    @Test
    void testDeleteIndex() throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("index_leon");
        AcknowledgedResponse acknowledgedResponse = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        System.out.println(acknowledgedResponse.toString());
        System.out.println(acknowledgedResponse.isAcknowledged());
    }

    /**
     * 测试判断指定索引是否存在，如果不存在则创建
     *
     * @throws IOException
     */
    @Test
    void testCreateIndexIfNotExist() throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest("index_leon");
        boolean exists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        if (!exists) {
            System.out.printf("index_leon 不存在，执行创建操作。。。");
            //1.创建索引请求
            CreateIndexRequest request = new CreateIndexRequest("index_leon");
            //2.客户端执行请求 CreateIndexRequest，返回响应
            CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
            System.out.println(createIndexResponse);
        }
    }

    /**
     * 添加文档
     */
    @Test
    void testAddDocument() throws IOException {
        // 6
        User user = new User("张三", 28);

        // 创建请求
        IndexRequest request = new IndexRequest("index_leon");

        // 规则 put/index_leon/_doc/1
        request.id("2");
        request.timeout("1s"); // 设置超时
//        request.timeout(TimeValue.timeValueMillis(10));

        // 将对象放入请求 json
        request.source(JSON.toJSONString(user), XContentType.JSON);

        // 发送请求，获取相应结果
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        System.out.println(indexResponse.toString());
        System.out.println(indexResponse.status()); // 对应命令返回的状态 CREATED
    }

    /**
     * 判断文档是否存在
     * get/index/doc/1
     */
    @Test
    void testIsDocumentExist() throws IOException {
        GetRequest getRequest = new GetRequest("index_leon", "1");
        // 不获取返回的 _source 的上下文
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");

        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     * 获取文档信息
     * get/index/doc/1
     */
    @Test
    void testGetDocument() throws IOException {
        GetRequest getRequest = new GetRequest("leon_index", "1");
        // 不获取返回的 _source 的上下文
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(getResponse.getSourceAsString()); // 打印文档的内容
        System.out.println(getResponse);
    }

    /**
     * 更新文档信息
     * get/index/doc/1
     */
    @Test
    void testUpdateDocument() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("leon_index", "1");
        updateRequest.timeout("1s");
        User user = new User("leon", 18);
        updateRequest.doc(JSON.toJSONString(user), XContentType.JSON);
        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(updateResponse.status());
    }

    /**
     * 删除文档信息
     * get/index/doc/1
     */
    @Test
    void testDeleteDocument() throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("leon_index", "1");
        deleteRequest.timeout("1s");
        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(deleteResponse.status());
    }

    /**
     * 批量插入文档信息
     * get/index/doc/1
     */
    @Test
    void testBulkAddDocument() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");

        List<User> userList = new ArrayList<>();
        for (int i = 20; i < 100; i++) {
            userList.add(new User("张三" + (i + 1), (10 + i)));
        }

        for (int i = 0; i < userList.size(); i++) {
            // 批量操作都在这儿
            bulkRequest.add(new IndexRequest("index_leon")
                    .id("" + (i + 1))
                    .source(JSON.toJSONString(userList.get(i)), XContentType.JSON));
        }

        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulkResponse.status());
        System.out.println(bulkResponse.hasFailures());
    }

    /**
     * 查询文档信息
     * SearchRequest 搜索请求
     * SearchSourceBuilder 条件构造
     * HighlightBuilder 高亮构造
     * TermQueryBuilder 精确查询
     * xxxQueryBuilder 对应命令行的查询
     */
    @Test
    void testSearchDocument() throws IOException {
        SearchRequest searchRequest = new SearchRequest("index_leon");
        // 构建搜索条件
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 查询条件可以用 QueryBuilders 构建

        // QueryBuilders.termQuery 精确查询
        // QueryBuilders.matchAllQuery 匹配所有
        // BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        MatchAllQueryBuilder matchAllQuery = QueryBuilders.matchAllQuery();
        sourceBuilder.query(matchAllQuery);
        // TermQueryBuilder termQuery = QueryBuilders.termQuery("name", "leon");
        // sourceBuilder.query(termQuery);

        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        sourceBuilder.from(0); // 设置分页
        sourceBuilder.size(6);

        // 将查询条件放入查询请求
        searchRequest.source(sourceBuilder);

        // 执行请求
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(searchResponse);
        System.out.println(JSON.toJSONString(searchResponse.getHits()));
        System.out.println("==================遍历======================");
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
    }
}

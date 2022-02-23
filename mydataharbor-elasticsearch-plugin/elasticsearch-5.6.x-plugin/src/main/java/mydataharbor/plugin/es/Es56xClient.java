package mydataharbor.plugin.es;

import lombok.extern.slf4j.Slf4j;
import mydataharbor.elasticsearch.common.sink.ElasticsearchSinkConfig;
import mydataharbor.elasticsearch.common.sink.ElasticsearchSinkReq;
import mydataharbor.sink.es.IEsClient;
import mydataharbor.sink.exception.EsException;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.io.stream.BytesStreamOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by xulang on 2021/7/27.
 */
@Slf4j
public class Es56xClient implements IEsClient {

  private RestHighLevelClient restHighLevelClient;

  private ElasticsearchSinkConfig elasticsearchSinkConfig;

  private RestClient restClient;

  public Es56xClient(ElasticsearchSinkConfig elasticsearchSinkConfig) {
    this.elasticsearchSinkConfig = elasticsearchSinkConfig;
    HttpHost[] httpHosts = elasticsearchSinkConfig.getEsIpPort().stream().map(str -> new HttpHost(str.split(":")[0], Integer.parseInt(str.split(":")[1]))).toArray(HttpHost[]::new);
    restClient = RestClient.builder(httpHosts)
      .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
        .setConnectTimeout((int) elasticsearchSinkConfig.getConnectTimeOut())
        .setSocketTimeout((int) elasticsearchSinkConfig.getSocketTimeOut()))
      .setMaxRetryTimeoutMillis(10000).build();
    this.restHighLevelClient = new RestHighLevelClient(restClient);

  }

  @Override
  public boolean checkIndexExist(String index) {
    try {
      Response response = restClient.performRequest(HttpHead.METHOD_NAME, index);
      if (response.getStatusLine().getStatusCode() == 200) {
        return true;
      } else {
        return false;
      }
    } catch (IOException e) {
      throw new EsException("检查索引是否存在时发生异常！", e);
    }
  }

  @Override
  public void createIndex(String index, Map settings, Map mapping) {
    BytesStreamOutput bytesStreamOutput = new BytesStreamOutput();
    CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
    createIndexRequest.settings(settings);
    createIndexRequest.mapping("_doc", mapping);
    try {
      createIndexRequest.writeTo(bytesStreamOutput);
    } catch (IOException e) {
      log.error("创建索引失败");
      throw new EsException("创建索引时发生错误！", e);
    }
    try {
      String content = new String(bytesStreamOutput.bytes().toBytesRef().bytes);
      restClient.performRequest(HttpPut.METHOD_NAME, index, new HashMap<>(), new StringEntity(content, ContentType.APPLICATION_JSON));
    } catch (IOException e) {
      throw new EsException("创建索引时发生错误！", e);
    }
  }

  @Override
  public Object write(ElasticsearchSinkReq elasticsearchSinkReq) throws Exception {
    switch (elasticsearchSinkReq.getWriteType()) {
      case DELETE:
        DeleteRequest deleteRequest = new DeleteRequest(elasticsearchSinkConfig.getWriteIndexConfig().getIndexName(), "_doc", elasticsearchSinkReq.getKey());
        return restHighLevelClient.delete(deleteRequest);
      case UPDATE:
        UpdateRequest updateRequest = new UpdateRequest(elasticsearchSinkConfig.getWriteIndexConfig().getIndexName(), "_doc", elasticsearchSinkReq.getKey()).doc(elasticsearchSinkReq.getSource());
        return restHighLevelClient.update(updateRequest);
      case INDEX:
        IndexRequest indexRequest = new IndexRequest(elasticsearchSinkConfig.getWriteIndexConfig().getIndexName(), "_doc", elasticsearchSinkReq.getKey()).source(elasticsearchSinkReq.getSource());
        return restHighLevelClient.index(indexRequest);
      default:
        throw new RuntimeException("impossable!");
    }
  }

  @Override
  public Object batchWrite(List<ElasticsearchSinkReq> elasticsearchSinkReqs) throws Exception {
    BulkRequest bulkRequest = new BulkRequest();
    for (ElasticsearchSinkReq elasticsearchSinkReq : elasticsearchSinkReqs) {
      DocWriteRequest docWriteRequest = null;
      switch (elasticsearchSinkReq.getWriteType()) {
        case DELETE:
          docWriteRequest = new DeleteRequest(elasticsearchSinkConfig.getWriteIndexConfig().getIndexName(), "_doc", elasticsearchSinkReq.getKey());
          break;
        case UPDATE:
          docWriteRequest = new UpdateRequest(elasticsearchSinkConfig.getWriteIndexConfig().getIndexName(), "_doc", elasticsearchSinkReq.getKey()).doc(elasticsearchSinkReq.getSource());
          break;
        case INDEX:
          docWriteRequest = new IndexRequest(elasticsearchSinkConfig.getWriteIndexConfig().getIndexName(), "_doc", elasticsearchSinkReq.getKey()).source(elasticsearchSinkReq.getSource());
          break;
        default:
          throw new RuntimeException("impossable!");
      }
      bulkRequest.add(docWriteRequest);
    }
    return restHighLevelClient.bulk(bulkRequest);
  }

  @Override
  public void close() {
    if (restClient != null) {
      try {
        restClient.close();
      } catch (IOException e) {
        log.error("关闭es客户端发生异常！", e);
      }
    }
  }
}

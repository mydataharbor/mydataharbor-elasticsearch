package mydataharbor.plugin.es.sink;


import mydataharbor.elasticsearch.common.sink.ElasticsearchSinkConfig;
import mydataharbor.plugin.es.Es68xClient;
import mydataharbor.sink.AbstractEsSink;
import mydataharbor.sink.es.IEsClient;

/**
 * Created by xulang on 2021/7/27.
 */
public class Es68xSink extends AbstractEsSink {
  public Es68xSink(ElasticsearchSinkConfig elasticsearchSinkConfig) {
    super(elasticsearchSinkConfig);
  }

  @Override
  public IEsClient initEsClient(ElasticsearchSinkConfig elasticsearchSinkConfig) {
    return new Es68xClient(elasticsearchSinkConfig);
  }

  @Override
  public String name() {
    return "es6.8.x 版本的写入器";
  }
}

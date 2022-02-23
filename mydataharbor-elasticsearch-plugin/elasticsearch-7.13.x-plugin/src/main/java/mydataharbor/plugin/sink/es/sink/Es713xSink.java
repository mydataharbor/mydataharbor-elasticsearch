package mydataharbor.plugin.sink.es.sink;


import mydataharbor.elasticsearch.common.sink.ElasticsearchSinkConfig;
import mydataharbor.plugin.sink.es.Es713xClient;
import mydataharbor.sink.AbstractEsSink;
import mydataharbor.sink.es.IEsClient;

/**
 * Created by xulang on 2021/7/27.
 */
public class Es713xSink extends AbstractEsSink {
  public Es713xSink(ElasticsearchSinkConfig elasticsearchSinkConfig) {
    super(elasticsearchSinkConfig);
  }

  @Override
  public IEsClient initEsClient(ElasticsearchSinkConfig elasticsearchSinkConfig) {
    return new Es713xClient(elasticsearchSinkConfig);
  }

  @Override
  public String name() {
    return "es7.7.x 版本的写入器";
  }
}

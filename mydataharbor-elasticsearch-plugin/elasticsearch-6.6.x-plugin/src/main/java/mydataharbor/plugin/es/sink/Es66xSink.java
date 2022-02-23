package mydataharbor.plugin.es.sink;

import mydataharbor.elasticsearch.common.sink.ElasticsearchSinkConfig;
import mydataharbor.plugin.es.Es66xClient;
import mydataharbor.sink.AbstractEsSink;
import mydataharbor.sink.es.IEsClient;

/**
 * Created by xulang on 2021/7/27.
 */
public class Es66xSink extends AbstractEsSink {
  public Es66xSink(ElasticsearchSinkConfig elasticsearchSinkConfig) {
    super(elasticsearchSinkConfig);
  }

  @Override
  public IEsClient initEsClient(ElasticsearchSinkConfig elasticsearchSinkConfig) {
    return new Es66xClient(elasticsearchSinkConfig);
  }

  @Override
  public String name() {
    return "es6.6.x 版本的写入器";
  }
}

package mydataharbor.plugin.es.sink;

import mydataharbor.elasticsearch.common.sink.ElasticsearchSinkConfig;
import mydataharbor.plugin.es.Es67xClient;
import mydataharbor.sink.AbstractEsSink;
import mydataharbor.sink.es.IEsClient;

/**
 * Created by xulang on 2021/7/27.
 */
public class Es67xSink extends AbstractEsSink {
  public Es67xSink(ElasticsearchSinkConfig elasticsearchSinkConfig) {
    super(elasticsearchSinkConfig);
  }

  @Override
  public IEsClient initEsClient(ElasticsearchSinkConfig elasticsearchSinkConfig) {
    return new Es67xClient(elasticsearchSinkConfig);
  }

  @Override
  public String name() {
    return "es6.7.x 版本的写入器";
  }
}

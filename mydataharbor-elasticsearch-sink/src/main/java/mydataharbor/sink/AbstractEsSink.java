package mydataharbor.sink;

import lombok.extern.slf4j.Slf4j;
import mydataharbor.IDataSink;
import mydataharbor.elasticsearch.common.sink.ElasticsearchSinkConfig;
import mydataharbor.elasticsearch.common.sink.ElasticsearchSinkReq;
import mydataharbor.exception.ResetException;
import mydataharbor.setting.BaseSettingContext;
import mydataharbor.sink.es.IEsClient;
import mydataharbor.sink.exception.EsException;

import java.io.IOException;
import java.net.ConnectException;
import java.util.List;

/**
 * es写入器
 * Created by xulang on 2021/7/26.
 */
@Slf4j
public abstract class AbstractEsSink implements IDataSink<ElasticsearchSinkReq, BaseSettingContext> {


  private IEsClient esClient;

  public AbstractEsSink(ElasticsearchSinkConfig elasticsearchSinkConfig) {
    IEsClient esClient = initEsClient(elasticsearchSinkConfig);
    this.esClient = esClient;
    if (elasticsearchSinkConfig.getWriteIndexConfig().isAutoCreate()) {
      //创建索引
      if (!esClient.checkIndexExist(elasticsearchSinkConfig.getWriteIndexConfig().getIndexName())) {
        synchronized (AbstractEsSink.class) {
          if (!esClient.checkIndexExist(elasticsearchSinkConfig.getWriteIndexConfig().getIndexName())) {
            esClient.createIndex(elasticsearchSinkConfig.getWriteIndexConfig().getIndexName(), elasticsearchSinkConfig.getWriteIndexConfig().getSettings(), elasticsearchSinkConfig.getWriteIndexConfig().getMapping());
          }
        }
      }
    }
  }

  /**
   * 初始化esClient客户端
   *
   * @param elasticsearchSinkConfig
   * @return
   */
  public abstract IEsClient initEsClient(ElasticsearchSinkConfig elasticsearchSinkConfig);


  @Override
  public WriterResult write(ElasticsearchSinkReq record, BaseSettingContext settingContext) throws ResetException {
    WriterResult.WriterResultBuilder writerResultBuilder = WriterResult.builder();
    try {
      Object writeResult = esClient.write(record);
      log.info("写入结果：{}", writeResult);
      writerResultBuilder.writeReturn(writeResult);
    } catch (Exception e) {
      if (e instanceof ConnectException) {
        throw new ResetException("连接异常", e);
      } else {
        throw new EsException("请求es发生异常", e);
      }
    }
    return writerResultBuilder.success(true).commit(true).msg("ok").build();
  }

  @Override
  public WriterResult write(List<ElasticsearchSinkReq> records, BaseSettingContext settingContext) throws ResetException {
    WriterResult.WriterResultBuilder writerResultBuilder = WriterResult.builder();
    try {
      Object writeResult = esClient.batchWrite(records);
      writerResultBuilder.writeReturn(writeResult);
    } catch (Exception e) {
      log.error("写入es发生异常！", e);
      if (e instanceof ConnectException) {
        throw new ResetException("写入es发生异常！", e);
      }
      throw new EsException("", e);
    }
    return writerResultBuilder.success(true).commit(true).msg("ok").build();
  }

  @Override
  public void close() throws IOException {
    if (esClient != null) {
      esClient.close();
    }
  }
}

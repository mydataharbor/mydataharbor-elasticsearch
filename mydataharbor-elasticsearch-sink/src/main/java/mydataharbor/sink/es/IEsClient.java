package mydataharbor.sink.es;


import mydataharbor.elasticsearch.common.sink.ElasticsearchSinkReq;

import java.util.List;
import java.util.Map;

/**
 * Created by xulang on 2021/7/27.
 */
public interface IEsClient {

  /**
   * 检查索引是否存在
   *
   * @param index
   * @return
   */
  boolean checkIndexExist(String index);

  /**
   * 创建索引
   *
   * @param index
   * @param settings
   * @param mapping
   * @return
   */
  void createIndex(String index, Map settings, Map mapping);

  /**
   * 单条写入
   *
   * @param elasticsearchSinkReq
   */
  Object write(ElasticsearchSinkReq elasticsearchSinkReq) throws Exception;

  /**
   * 批量写入
   *
   * @param elasticsearchSinkReqs
   */
  Object batchWrite(List<ElasticsearchSinkReq> elasticsearchSinkReqs) throws Exception;

  /**
   * 关闭资源
   */
  void close();

}

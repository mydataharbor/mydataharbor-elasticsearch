package mydataharbor.elasticsearch.common.sink;

import lombok.Data;

import java.util.Map;

/**
 * Created by xulang on 2021/7/26.
 */
@Data
public class ElasticsearchSinkReq {

  /**
   * 写入方式
   */
  private EsWriteType writeType;

  /**
   * 数据key
   */
  private String key;

  /**
   * 数据体
   */
  private Map<String, Object> source;

  /**
   * 消息产生时间
   */
  private long messageCreateTime;
}

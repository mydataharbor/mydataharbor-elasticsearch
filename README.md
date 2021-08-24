# mydataharbor-elasticsearch
# 作者

MyDataHarbor([1053618636@qq.com](mailto:1053618636@qq.com))

# 项目介绍

该项目是为MyDataHarbor实现elasticsearch的DataSource 和 Sink，让使用者可以从elasticsearch抽取数据，或者将数据写入elasticsearch。

# 实现版本

| 中间件/协议   | 数据源（DataSource） | 写入源（Sink）                                          |
| ------------- | -------------------- | ------------------------------------------------------- |
| elasticsearch | 计划中               | ✅5.6.x✅6.4.x✅6.0.x✅6.8.x✅6.5.x✅6.6.x✅6.7.x✅7.7.x✅7.13.x |

# 配置

## DataSource配置

```json
待实现
```

## Sink配置

```java
  @MyDataHarborMarker(title = "es连接ip信息", des = "如：[127.0.0.1:9400,127.0.0.1:9500]")
  private List<String> esIpPort;

  @MyDataHarborMarker(title = "连接超时时间", des = "默认2s", require = false)
  private long connectTimeOut = 2000;

  @MyDataHarborMarker(title = "通讯超时时间", des = "默认5s", require = false)
  private long socketTimeOut = 5000;

  @MyDataHarborMarker(title = "是否需要授权连接", require = false)
  private boolean enableAuth = false;

  @MyDataHarborMarker(title = "用户名", require = false)
  private String userName;

  @MyDataHarborMarker(title = "密码", require = false)
  private String password;

  @MyDataHarborMarker(title = "写入索引配置")
  private WriteIndexConfig writeIndexConfig;
```


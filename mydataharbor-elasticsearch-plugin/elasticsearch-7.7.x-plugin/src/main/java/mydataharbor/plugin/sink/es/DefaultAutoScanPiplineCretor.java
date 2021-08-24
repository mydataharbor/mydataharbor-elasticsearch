package mydataharbor.plugin.sink.es;

import mydataharbor.IDataPipline;
import mydataharbor.plugin.base.creator.AbstractAutoScanPiplineCreator;
import mydataharbor.setting.BaseSettingContext;
import org.pf4j.Extension;
import org.pf4j.ExtensionPoint;

import java.util.Map;

/**
 * Created by xulang on 2021/8/17.
 */
@Extension
public class DefaultAutoScanPiplineCretor extends AbstractAutoScanPiplineCreator<Map<String, Object>, BaseSettingContext>implements ExtensionPoint {
  @Override
  public String scanPackage() {
    return "mydataharbor";
  }

  @Override
  public String type() {
    return "ES 6.8.x 组件扫描器";
  }

  @Override
  public IDataPipline createPipline(Map<String, Object> config, BaseSettingContext settingContext) throws Exception {
    throw new RuntimeException("改创建器无法创建pipline");
  }

  @Override
  public boolean canCreatePipline() {
    return false;
  }
}

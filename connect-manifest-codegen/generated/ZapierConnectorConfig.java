package io.kafka.connect.generated;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public final class ZapierConnectorConfig extends AbstractConfig {
  public static final String SECRET_CONFIG = "secret";

  public ZapierConnectorConfig(Map<? extends String, ?> originals) {
    super(config(), originals);
  }

  public static ConfigDef config() {
    ConfigDef def = new ConfigDef();
    def.define(SECRET_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Secret key supplied by zapier");
    return def;
  }

  public String getSecret() {
    return getString(SECRET_CONFIG);
  }
}

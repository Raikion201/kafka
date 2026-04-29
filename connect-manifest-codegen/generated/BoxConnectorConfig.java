package io.kafka.connect.generated;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public final class BoxConnectorConfig extends AbstractConfig {
  public static final String ACCESS_TOKEN_CONFIG = "access_token";

  public BoxConnectorConfig(Map<? extends String, ?> originals) {
    super(config(), originals);
  }

  public static ConfigDef config() {
    ConfigDef def = new ConfigDef();
    def.define(ACCESS_TOKEN_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Developer Token (or OAuth access token)");
    return def;
  }

  public String getAccessToken() {
    return getString(ACCESS_TOKEN_CONFIG);
  }
}

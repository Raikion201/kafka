package io.kafka.connect.generated;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public final class GoogleAnalyticsJwtConnectorConfig extends AbstractConfig {
  public static final String CREDENTIALS_JSON_CONFIG = "credentials_json";

  public GoogleAnalyticsJwtConnectorConfig(Map<? extends String, ?> originals) {
    super(config(), originals);
  }

  public static ConfigDef config() {
    ConfigDef def = new ConfigDef();
    def.define(CREDENTIALS_JSON_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Contents of the Google Service Account JSON key file.");
    return def;
  }

  public String getCredentialsJson() {
    return getString(CREDENTIALS_JSON_CONFIG);
  }
}

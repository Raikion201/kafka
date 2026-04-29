package io.kafka.connect.generated;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public final class TogglConnectorConfig extends AbstractConfig {
  public static final String API_TOKEN_CONFIG = "api_token";

  public static final String API_PASSWORD_CONFIG = "api_password";

  public TogglConnectorConfig(Map<? extends String, ?> originals) {
    super(config(), originals);
  }

  public static ConfigDef config() {
    ConfigDef def = new ConfigDef();
    def.define(API_TOKEN_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Your Toggl Track API token. Find it under Profile Settings at https://track.toggl.com/profile (scroll to \"API Token\", click reveal).");
    def.define(API_PASSWORD_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Toggl uses a fixed literal string as the Basic Auth password. Always enter the exact value: api_token");
    return def;
  }

  public String getApiToken() {
    return getString(API_TOKEN_CONFIG);
  }

  public String getApiPassword() {
    return getString(API_PASSWORD_CONFIG);
  }
}

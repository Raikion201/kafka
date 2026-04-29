package io.kafka.connect.generated;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public final class MetabaseConnectorConfig extends AbstractConfig {
  public static final String INSTANCE_API_URL_CONFIG = "instance_api_url";

  public static final String USERNAME_CONFIG = "username";

  public static final String PASSWORD_CONFIG = "password";

  public MetabaseConnectorConfig(Map<? extends String, ?> originals) {
    super(config(), originals);
  }

  public static ConfigDef config() {
    ConfigDef def = new ConfigDef();
    def.define(INSTANCE_API_URL_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Instance API URL");
    def.define(USERNAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Username");
    def.define(PASSWORD_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Password");
    return def;
  }

  public String getInstanceApiUrl() {
    return getString(INSTANCE_API_URL_CONFIG);
  }

  public String getUsername() {
    return getString(USERNAME_CONFIG);
  }

  public String getPassword() {
    return getString(PASSWORD_CONFIG);
  }
}

package io.kafka.connect.generated;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public final class BoxConnectorConfig extends AbstractConfig {
  public static final String CLIENT_ID_CONFIG = "client_id";

  public static final String CLIENT_SECRET_CONFIG = "client_secret";

  public static final String USER_CONFIG = "user";

  public BoxConnectorConfig(Map<? extends String, ?> originals) {
    super(config(), originals);
  }

  public static ConfigDef config() {
    ConfigDef def = new ConfigDef();
    def.define(CLIENT_ID_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "OAuth Client ID");
    def.define(CLIENT_SECRET_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "OAuth Client Secret");
    def.define(USER_CONFIG, ConfigDef.Type.LONG, ConfigDef.Importance.HIGH, "User");
    return def;
  }

  public String getClientId() {
    return getString(CLIENT_ID_CONFIG);
  }

  public String getClientSecret() {
    return getString(CLIENT_SECRET_CONFIG);
  }

  public String getUser() {
    return getString(USER_CONFIG);
  }
}

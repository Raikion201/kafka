package io.kafka.connect.generated;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public final class AkeneoConnectorConfig extends AbstractConfig {
  public static final String HOST_CONFIG = "host";

  public static final String API_USERNAME_CONFIG = "api_username";

  public static final String PASSWORD_CONFIG = "password";

  public static final String CLIENT_ID_CONFIG = "client_id";

  public static final String SECRET_CONFIG = "secret";

  public AkeneoConnectorConfig(Map<? extends String, ?> originals) {
    super(config(), originals);
  }

  public static ConfigDef config() {
    ConfigDef def = new ConfigDef();
    def.define(HOST_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "https://cb8715249e.trial.akeneo.cloud");
    def.define(API_USERNAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "API Username");
    def.define(PASSWORD_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Password");
    def.define(CLIENT_ID_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Client ID");
    def.define(SECRET_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.MEDIUM, "Secret");
    return def;
  }

  public String getHost() {
    return getString(HOST_CONFIG);
  }

  public String getApiUsername() {
    return getString(API_USERNAME_CONFIG);
  }

  public String getPassword() {
    return getString(PASSWORD_CONFIG);
  }

  public String getClientId() {
    return getString(CLIENT_ID_CONFIG);
  }

  public String getSecret() {
    return getString(SECRET_CONFIG);
  }
}

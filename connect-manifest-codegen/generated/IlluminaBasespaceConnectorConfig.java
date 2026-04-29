package io.kafka.connect.generated;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public final class IlluminaBasespaceConnectorConfig extends AbstractConfig {
  public static final String ACCESS_TOKEN_CONFIG = "access_token";

  public static final String DOMAIN_CONFIG = "domain";

  public static final String USER_CONFIG = "user";

  public IlluminaBasespaceConnectorConfig(Map<? extends String, ?> originals) {
    super(config(), originals);
  }

  public static ConfigDef config() {
    ConfigDef def = new ConfigDef();
    def.define(ACCESS_TOKEN_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "BaseSpace access token. Instructions for obtaining your access token can be found in the BaseSpace Developer Documentation.");
    def.define(DOMAIN_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Domain name of the BaseSpace instance (e.g., euw2.sh.basespace.illumina.com)");
    def.define(USER_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Providing a user ID restricts the returned data to what that user can access. If you use the default ('current'), all data accessible to the user associated with the API key will be shown.");
    return def;
  }

  public String getAccessToken() {
    return getString(ACCESS_TOKEN_CONFIG);
  }

  public String getDomain() {
    return getString(DOMAIN_CONFIG);
  }

  public String getUser() {
    return getString(USER_CONFIG);
  }
}

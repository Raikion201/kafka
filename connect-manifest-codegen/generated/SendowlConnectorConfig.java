package io.kafka.connect.generated;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public final class SendowlConnectorConfig extends AbstractConfig {
  public static final String USERNAME_CONFIG = "username";

  public static final String PASSWORD_CONFIG = "password";

  public static final String START_DATE_CONFIG = "start_date";

  public SendowlConnectorConfig(Map<? extends String, ?> originals) {
    super(config(), originals);
  }

  public static ConfigDef config() {
    ConfigDef def = new ConfigDef();
    def.define(USERNAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Enter you API Key");
    def.define(PASSWORD_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.MEDIUM, "Enter your API secret");
    def.define(START_DATE_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Start date");
    return def;
  }

  public String getUsername() {
    return getString(USERNAME_CONFIG);
  }

  public String getPassword() {
    return getString(PASSWORD_CONFIG);
  }

  public String getStartDate() {
    return getString(START_DATE_CONFIG);
  }
}

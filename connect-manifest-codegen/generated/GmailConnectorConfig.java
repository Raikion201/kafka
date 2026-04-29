package io.kafka.connect.generated;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public final class GmailConnectorConfig extends AbstractConfig {
  public static final String CLIENT_ID_CONFIG = "client_id";

  public static final String CLIENT_SECRET_CONFIG = "client_secret";

  public static final String CLIENT_REFRESH_TOKEN_CONFIG = "client_refresh_token";

  public static final String INCLUDE_SPAM_AND_TRASH_CONFIG = "include_spam_and_trash";

  public GmailConnectorConfig(Map<? extends String, ?> originals) {
    super(config(), originals);
  }

  public static ConfigDef config() {
    ConfigDef def = new ConfigDef();
    def.define(CLIENT_ID_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Your Google OAuth 2.0 Client ID from the Google Cloud Console (APIs & Services > Credentials).");
    def.define(CLIENT_SECRET_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Your Google OAuth 2.0 Client Secret from the Google Cloud Console.");
    def.define(CLIENT_REFRESH_TOKEN_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Your Google OAuth 2.0 Refresh Token obtained after completing the OAuth authorization flow.");
    def.define(INCLUDE_SPAM_AND_TRASH_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.MEDIUM, "Set to \"true\" to include messages from SPAM and TRASH folders in message and thread streams. Defaults to \"false\".");
    return def;
  }

  public String getClientId() {
    return getString(CLIENT_ID_CONFIG);
  }

  public String getClientSecret() {
    return getString(CLIENT_SECRET_CONFIG);
  }

  public String getClientRefreshToken() {
    return getString(CLIENT_REFRESH_TOKEN_CONFIG);
  }

  public String getIncludeSpamAndTrash() {
    return getString(INCLUDE_SPAM_AND_TRASH_CONFIG);
  }
}

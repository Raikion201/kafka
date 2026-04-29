package io.kafka.connect.generated;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public final class AssemblyaiConnectorConfig extends AbstractConfig {
  public static final String API_KEY_CONFIG = "api_key";

  public static final String START_DATE_CONFIG = "start_date";

  public static final String SUBTITLE_FORMAT_CONFIG = "subtitle_format";

  public static final String REQUEST_ID_CONFIG = "request_id";

  public AssemblyaiConnectorConfig(Map<? extends String, ?> originals) {
    super(config(), originals);
  }

  public static ConfigDef config() {
    ConfigDef def = new ConfigDef();
    def.define(API_KEY_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Your AssemblyAI API key. You can find it in the AssemblyAI dashboard at https://www.assemblyai.com/app/api-keys.");
    def.define(START_DATE_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Start date");
    def.define(SUBTITLE_FORMAT_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "The subtitle format for transcript_subtitle stream");
    def.define(REQUEST_ID_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.MEDIUM, "The request ID for LeMur responses");
    return def;
  }

  public String getApiKey() {
    return getString(API_KEY_CONFIG);
  }

  public String getStartDate() {
    return getString(START_DATE_CONFIG);
  }

  public String getSubtitleFormat() {
    return getString(SUBTITLE_FORMAT_CONFIG);
  }

  public String getRequestId() {
    return getString(REQUEST_ID_CONFIG);
  }
}

package io.kafka.connect.generated;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public final class GoogleCloudStorageJwtConnectorConfig extends AbstractConfig {
  public static final String CREDENTIALS_JSON_CONFIG = "credentials_json";

  public static final String BUCKET_NAME_CONFIG = "bucket_name";

  public GoogleCloudStorageJwtConnectorConfig(Map<? extends String, ?> originals) {
    super(config(), originals);
  }

  public static ConfigDef config() {
    ConfigDef def = new ConfigDef();
    def.define(CREDENTIALS_JSON_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Contents of the Google Service Account JSON key file.");
    def.define(BUCKET_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Name of the GCS bucket to list objects from.");
    return def;
  }

  public String getCredentialsJson() {
    return getString(CREDENTIALS_JSON_CONFIG);
  }

  public String getBucketName() {
    return getString(BUCKET_NAME_CONFIG);
  }
}

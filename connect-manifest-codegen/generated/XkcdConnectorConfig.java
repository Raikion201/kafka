package io.kafka.connect.generated;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public final class XkcdConnectorConfig extends AbstractConfig {
  public static final String COMIC_NUMBER_CONFIG = "comic_number";

  public XkcdConnectorConfig(Map<? extends String, ?> originals) {
    super(config(), originals);
  }

  public static ConfigDef config() {
    ConfigDef def = new ConfigDef();
    def.define(COMIC_NUMBER_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "The xkcd comic number to fetch (e.g. 614 for the \"Woodpecker\" comic)");
    return def;
  }

  public String getComicNumber() {
    return getString(COMIC_NUMBER_CONFIG);
  }
}

package io.kafka.connect.generated;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public final class YahooFinancePriceConnectorConfig extends AbstractConfig {
  public static final String TICKERS_CONFIG = "tickers";
  public static final String INTERVAL_CONFIG = "interval";
  public static final String RANGE_CONFIG = "range";

  public YahooFinancePriceConnectorConfig(Map<? extends String, ?> originals) {
    super(config(), originals);
  }

  public static ConfigDef config() {
    ConfigDef def = new ConfigDef();
    def.define(TICKERS_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
        "Comma-separated stock ticker symbols to fetch, e.g. AAPL,MSFT,GOOGL. Whitespace around commas is allowed.");
    def.define(INTERVAL_CONFIG, ConfigDef.Type.STRING, "1d", ConfigDef.Importance.MEDIUM,
        "Price data interval. Allowed values: 1m, 5m, 15m, 30m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo.");
    def.define(RANGE_CONFIG, ConfigDef.Type.STRING, "1mo", ConfigDef.Importance.MEDIUM,
        "Price data range. Allowed values: 1d, 5d, 7d, 1mo, 3mo, 6mo, 1y, 2y, 5y, ytd, max.");
    return def;
  }

  public String getTickers() {
    return getString(TICKERS_CONFIG);
  }

  public String getInterval() {
    return getString(INTERVAL_CONFIG);
  }

  public String getRange() {
    return getString(RANGE_CONFIG);
  }
}

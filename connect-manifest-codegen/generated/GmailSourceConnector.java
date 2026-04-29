package io.kafka.connect.generated;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

public final class GmailSourceConnector extends SourceConnector {
  private Map<String, String> props;

  private GmailConnectorConfig config;

  @Override
  public String version() {
    return "1.0.0";
  }

  @Override
  public void start(Map<String, String> props) {
    this.props = props;
    this.config = new GmailConnectorConfig(props);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return GmailSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    return Collections.singletonList(props);
  }

  @Override
  public void stop() {
    // no persistent resources to release
  }

  @Override
  public ConfigDef config() {
    return GmailConnectorConfig.config();
  }
}

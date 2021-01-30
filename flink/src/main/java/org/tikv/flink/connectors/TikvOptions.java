package org.tikv.flink.connectors;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class TikvOptions {
  private TikvOptions() {}
  ;

  public static ConfigOption<String> PDADDRESS =
      ConfigOptions.key("pdaddress").stringType().noDefaultValue().withDescription("PD address");

  public static ConfigOption<String> DATABASE =
      ConfigOptions.key("database").stringType().noDefaultValue().withDescription("Database name");

  public static ConfigOption<String> TABLE =
      ConfigOptions.key("table").stringType().noDefaultValue().withDescription("table name");
}

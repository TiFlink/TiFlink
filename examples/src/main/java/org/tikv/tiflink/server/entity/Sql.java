package org.tikv.tiflink.server.entity;

public class Sql {

  private String catalog;
  private String ddl;
  private String dml;
  private String dql;

  public String getCatalog() {
    return catalog;
  }

  public void setCatalog(String catalog) {
    this.catalog = catalog;
  }

  public String getDdl() {
    return ddl;
  }

  public void setDdl(String ddl) {
    this.ddl = ddl;
  }

  public String getDml() {
    return dml;
  }

  public void setDml(String dml) {
    this.dml = dml;
  }

  public String getDql() {
    return dql;
  }

  public void setDql(String dql) {
    this.dql = dql;
  }
}

package org.tikv.tiflink;

import org.tikv.flink.connectors.TiFlinkApp;

public class TiFlinkExample {
  public static void main(final String[] args) throws Exception {
    final TiFlinkApp.Builder appBuilder =
        TiFlinkApp.newBuilder()
            .setJdbcUrl("jdbc:mysql://root@localhost:4000/test")
            .setQuery(
                "select id, "
                    + "first_name, "
                    + "last_name, "
                    + "email, "
                    + "(select count(*) from posts where author_id = authors.id) as posts "
                    + "from authors")
            .setTargetTable("authors_posts");
    try (final TiFlinkApp app = appBuilder.build()) {
      app.start();
    }
  }
}

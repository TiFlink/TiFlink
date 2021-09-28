package org.tikv.tiflink;

import org.tikv.flink.TiFlinkApp;

public class TiFlinkExample {
  public static void main(final String[] args) throws Exception {
    final TiFlinkApp.Builder appBuilder =
        TiFlinkApp.newBuilder()
            .setJdbcUrl("jdbc:mysql://root@localhost:4000/test") // Please make sure the user has correct permission
            .setQuery(
                "select id, "
                    + "first_name, "
                    + "last_name, "
                    + "email, "
                    + "(select count(*) from posts where author_id = authors.id) as posts "
                    + "from authors")
            // .setColumnNames("a", "b", "c", "d") // Override column names inferred from the query
            // .setPrimaryKeys("a") // Specify the primary key columns, defaults to the first column
            // .setDefaultDatabase("test") // Default TiDB database to use, defaults to that specified by JDBC URL
            .setTargetTable("author_posts") // TiFlink will automatically create the table if not exist
            // .setTargetTable("test", "author_posts") // It is possible to sepecify the full table path
            .setParallelism(3) // Parallelism of the Flink Job
            .setCheckpointInterval(1000) // Checkpoint interval in milliseconds. This interval determines data refresh rate
            .setDropOldTable(true) // If TiFlink should drop old target table on start
            .setForceNewTable(true); // If to throw an error if the target table already exists
    try (final TiFlinkApp app = appBuilder.build()) {
      app.start();
    }
  }
}

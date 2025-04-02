package com.example.lookup;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.FunctionContext;

/**
 * An async scalar function that performs lookups against a MySQL database. This function is used
 * for enriching product data with additional details from MySQL.
 */
@FunctionHint(output = @DataTypeHint("STRING"))
public class MySqlAsyncScalarFunction extends AsyncScalarFunction {
  private final String url;
  private final String username;
  private final String password;
  private final String tableName;
  private final String keyColumn;
  private final String valueColumn;
  private ExecutorService executorService;
  private Connection connection;
  private PreparedStatement preparedStatement;

  public MySqlAsyncScalarFunction(
      String host,
      int port,
      String database,
      String username,
      String password,
      String tableName,
      String keyColumn,
      String valueColumn) {
    this.url = String.format("jdbc:mysql://%s:%d/%s", host, port, database);
    this.username = username;
    this.password = password;
    this.tableName = tableName;
    this.keyColumn = keyColumn;
    this.valueColumn = valueColumn;
  }

  @Override
  public void open(FunctionContext context) throws Exception {
    // Create executor service after deserialization
    this.executorService = Executors.newFixedThreadPool(10);

    // Create database connection
    this.connection = DriverManager.getConnection(url, username, password);
    String query =
        String.format("SELECT %s FROM %s WHERE %s = ?", valueColumn, tableName, keyColumn);
    this.preparedStatement = connection.prepareStatement(query);
  }

  public void eval(CompletableFuture<String> future, String key) {
    if (key == null) {
      future.complete(null);
      return;
    }

    executorService.execute(
        () -> {
          try {
            preparedStatement.setString(1, key);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
              if (resultSet.next()) {
                future.complete(resultSet.getString(valueColumn));
              } else {
                future.complete(null);
              }
            }
          } catch (SQLException e) {
            future.completeExceptionally(e);
          }
        });
  }

  @Override
  public void close() throws Exception {
    if (preparedStatement != null) {
      preparedStatement.close();
    }
    if (connection != null) {
      connection.close();
    }
    if (executorService != null) {
      executorService.shutdown();
    }
  }
}

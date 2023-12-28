/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.dolphindb;

import com.dolphindb.jdbc.Driver;
import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.ConnectionFactory;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcIdentity;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.plugin.jdbc.ReadMapping;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.google.common.base.Strings.emptyToNull;
import static java.lang.String.format;

public class DolphinDBClient
        extends BaseJdbcClient
{
    @Inject
    public DolphinDBClient(JdbcConnectorId connectorId, BaseJdbcConfig config, DolphinDBConfig dolphindbConfig)
            throws SQLException
    {
        super(connectorId, config, "", connectionFactory(config, dolphindbConfig));
    }

    private static ConnectionFactory connectionFactory(BaseJdbcConfig config, DolphinDBConfig dolphindbConfig)
            throws SQLException
    {
        Properties connectionProperties = basicConnectionProperties(config);
        connectionProperties.setProperty("waitingTime", String.valueOf(dolphindbConfig.getWaitingTime()));
        connectionProperties.setProperty("allowMultiQueries", String.valueOf(dolphindbConfig.getAllowMultiQueries()));
        connectionProperties.setProperty("enableHighAvailability", String.valueOf(dolphindbConfig.getEnableHighAvailability()));
        return new DriverConnectionFactory(
                new Driver(),
                config.getConnectionUrl(),
                Optional.ofNullable(config.getUserCredentialName()),
                Optional.ofNullable(config.getPasswordCredentialName()),
                connectionProperties);
    }

    @Override
    protected Collection<String> listSchemas(Connection connection)
    {
        try (ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_CATALOG");
                schemaNames.add(schemaName);
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        return metadata.getTables(
                schemaName.orElse(null),
                null,
                escapeNamePattern(tableName, escape).orElse(null),
                null);
    }

    @Override
    protected String getTableSchemaName(ResultSet resultSet)
            throws SQLException
    {
        return resultSet.getString("TABLE_CAT");
    }

    private static ResultSet getColumns(JdbcTableHandle tableHandle, DatabaseMetaData metadata)
            throws SQLException
    {
        // 临时方案，后续需要修改
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        return metadata.getColumns(
                tableHandle.getCatalogName(),
                escapeNamePattern(Optional.ofNullable(tableHandle.getSchemaName()), escape).orElse(null),
                escapeNamePattern(Optional.ofNullable(tableHandle.getTableName()), escape).orElse(null),
                null);
    }

    @Override
    public PreparedStatement getPreparedStatement(ConnectorSession session, Connection connection, String sql)
            throws SQLException
    {
        // 去掉()，临时方案，因为preparedStatement的where语句使用()有bug
        sql = sql.replaceAll("[()]", "");

        // select null from -> select * from
        sql = sql.replaceAll("(?i)\\bSELECT\\s+NULL\\s+FROM\\b", "SELECT * FROM");

        // 直接替换 "/*" 为 " /*"
        sql = sql.replace("/*", " /*");

        // 使用正则表达式匹配 database.table
        Pattern pattern = Pattern.compile("from\\s+(\\w+://[^\\s.]+)\\.([^\\s.]+)\\s+", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(sql);

        // 进行替换
        sql = matcher.replaceAll("FROM loadTable(\"$1\", \"$2\") ");
        PreparedStatement statement = connection.prepareStatement(sql);
        return statement;
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        // 临时方案，后续需要修改
        try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session))) {
            try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData())) {
                int allColumns = 0;
                List<JdbcColumnHandle> columns = new ArrayList<>();
                while (resultSet.next()) {
                    allColumns++;
                    JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                            resultSet.getInt("DATA_TYPE"),
                            resultSet.getString("TYPE_NAME"),
                            8,
                            0);
                    Optional<ReadMapping> columnMapping = toPrestoType(session, typeHandle);
                    // skip unsupported column types
                    if (columnMapping.isPresent()) {
                        String columnName = resultSet.getString("COLUMN_NAME");
//                        boolean nullable = columnNullable == resultSet.getInt("NULLABLE");
                        boolean nullable = true;
                        Optional<String> comment = Optional.ofNullable(emptyToNull(resultSet.getString("REMARKS")));
                        columns.add(new JdbcColumnHandle(connectorId, columnName, typeHandle, columnMapping.get().getType(), nullable, comment));
                    }
                }
                if (columns.isEmpty()) {
                    // A table may have no supported columns. In rare cases (e.g. PostgreSQL) a table might have no columns at all.
                    // Throw an exception if the table has no supported columns.
                    // This can occur if all columns in the table are of unsupported types, or in rare cases, if the table has no columns at all.
                    throw new TableNotFoundException(
                            tableHandle.getSchemaTableName(),
                            format("Table '%s' has no supported columns (all %s columns are not supported)", tableHandle.getSchemaTableName(), allColumns));
                }
                return ImmutableList.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }
}

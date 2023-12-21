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
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;

import javax.inject.Inject;

import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.presto.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;

public class DolphinDBClient
        extends BaseJdbcClient
{
    @Inject
    public DolphinDBClient(JdbcConnectorId connectorId, BaseJdbcConfig config, DolphinDBConfig dolphindbConfig)
            throws SQLException
    {
        super(connectorId, config, "`", connectionFactory(config, dolphindbConfig));
    }

    private static ConnectionFactory connectionFactory(BaseJdbcConfig config, DolphinDBConfig dolphindbConfig)
            throws SQLException
    {
        Properties connectionProperties = basicConnectionProperties(config);
        if (dolphindbConfig.isAutoReconnect()) {
            connectionProperties.setProperty("autoReconnect", String.valueOf(dolphindbConfig.isAutoReconnect()));
        }
        return new DriverConnectionFactory(
                new Driver(),
                config.getConnectionUrl(),
                Optional.ofNullable(config.getUserCredentialName()),
                Optional.ofNullable(config.getPasswordCredentialName()),
                connectionProperties);
    }
}

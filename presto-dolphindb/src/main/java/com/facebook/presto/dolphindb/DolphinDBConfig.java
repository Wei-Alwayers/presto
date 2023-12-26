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

import com.facebook.airlift.configuration.Config;

public class DolphinDBConfig
{
    private boolean allowMultiQueries;
    private boolean enableHighAvailability;
    private int waitingTime = 3;

    public int getWaitingTime()
    {
        return waitingTime;
    }

    @Config("dolphindb.waitingTime")
    public DolphinDBConfig setWaitingTime(int connectionTimeout)
    {
        this.waitingTime = connectionTimeout;
        return this;
    }

    public boolean getAllowMultiQueries()
    {
        return allowMultiQueries;
    }

    @Config("dolphindb.allowMultiQueries")
    public DolphinDBConfig setAllowMultiQueries(boolean allowMultiQueries)
    {
        this.allowMultiQueries = allowMultiQueries;
        return this;
    }

    public boolean getEnableHighAvailability()
    {
        return enableHighAvailability;
    }

    @Config("dolphindb.enableHighAvailability")
    public DolphinDBConfig setEnableHighAvailability(boolean enableHighAvailability)
    {
        this.enableHighAvailability = enableHighAvailability;
        return this;
    }
}

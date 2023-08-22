/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Newtonsoft.Json;
using NodaTime;
using QuantConnect.Data;
using QuantConnect.Util;

namespace QuantConnect.DataSource
{
    /// <summary>
    /// Example custom data type
    /// </summary>
    public class QuiverTwitterFollowers : BaseData
    {
        private static readonly TimeSpan _period = TimeSpan.FromDays(1);

        /// <summary>
        /// Number of followers of the company's Twitter page on the given date
        /// </summary>
        [JsonProperty(PropertyName = "Followers")]
        public int Followers { get; set; }

        /// <summary>
        /// Day-over-day change in company's follower count
        /// </summary>
        [JsonProperty(PropertyName = "pct_change_daily")]
        public decimal DayPercentChange { get; set; }

        /// <summary>
        /// Week-over-week change in company's follower count
        /// </summary>
        [JsonProperty(PropertyName = "pct_change_week")]
        public decimal WeekPercentChange { get; set; }

        /// <summary>
        /// Month-over-month change in company's follower count
        /// </summary>
        [JsonProperty(PropertyName = "pct_change")]
        public decimal MonthPercentChange { get; set; }

        /// <summary>
        /// Current time marker of this data packet.
        /// </summary>
        /// <remarks>All data is timeseries based.</remarks>
        [JsonProperty(PropertyName = "Date")]
        [JsonConverter(typeof(DateTimeJsonConverter), "yyyy-MM-dd")]
        public new DateTime Time { get; set; }

        /// <summary>
        /// Time the data became available
        /// </summary>
        public override DateTime EndTime => Time + _period;

        /// <summary>
        /// Return the URL string source of the file. This will be converted to a stream
        /// </summary>
        /// <param name="config">Configuration object</param>
        /// <param name="date">Date of this source file</param>
        /// <param name="isLiveMode">true if we're in live mode, false for backtesting mode</param>
        /// <returns>String URL of source file.</returns>
        public override SubscriptionDataSource GetSource(SubscriptionDataConfig config, DateTime date, bool isLiveMode)
        {
            return new SubscriptionDataSource(
                Path.Combine(
                    Globals.DataFolder,
                    "alternative",
                    "quiver",
                    "twitter",
                    $"{config.Symbol.Value.ToLowerInvariant()}.csv"
                ),
                SubscriptionTransportMedium.LocalFile
            );
        }

        /// <summary>
        /// Parses the data from the line provided and loads it into LEAN
        /// </summary>
        /// <param name="config">Subscription configuration</param>
        /// <param name="line">Line of data</param>
        /// <param name="date">Date</param>
        /// <param name="isLiveMode">Is live mode</param>
        /// <returns>New instance</returns>
        public override BaseData Reader(SubscriptionDataConfig config, string line, DateTime date, bool isLiveMode)
        {
            var csv = line.Split(',');
            var followers = Parse.Int(csv[1]);

            return new QuiverTwitterFollowers
            {
                Symbol = config.Symbol,
                Time = Parse.DateTimeExact(csv[0], "yyyyMMdd"),
                Value = followers,
 
                Followers = followers,
                DayPercentChange = decimal.Parse(csv[2], NumberStyles.Any, CultureInfo.InvariantCulture),
                WeekPercentChange = decimal.Parse(csv[3], NumberStyles.Any, CultureInfo.InvariantCulture),
                MonthPercentChange = decimal.Parse(csv[4], NumberStyles.Any, CultureInfo.InvariantCulture)
            };
        }

        /// <summary>
        /// Clones the data
        /// </summary>
        /// <returns>A clone of the object</returns>
        public override BaseData Clone()
        {
            return new QuiverTwitterFollowers
            {
                Symbol = Symbol,
                Time = Time,
                Value = Followers,
                Followers = Followers,
                DayPercentChange = DayPercentChange,
                WeekPercentChange = WeekPercentChange,
                MonthPercentChange = MonthPercentChange
            };
        }

        /// <summary>
        /// Indicates whether the data source is tied to an underlying symbol and requires that corporate events be applied to it as well, such as renames and delistings
        /// </summary>
        /// <returns>false</returns>
        public override bool RequiresMapping()
        {
            return true;
        }

        /// <summary>
        /// Indicates whether the data is sparse.
        /// If true, we disable logging for missing files
        /// </summary>
        /// <returns>true</returns>
        public override bool IsSparseData()
        {
            return true;
        }

        /// <summary>
        /// Converts the instance to string
        /// </summary>
        public override string ToString()
        {
            return $"{Symbol} - Follower count: {Followers}";
        }

        /// <summary>
        /// Gets the default resolution for this data and security type
        /// </summary>
        public override Resolution DefaultResolution()
        {
            return Resolution.Daily;
        }

        /// <summary>
        /// Gets the supported resolution for this data and security type
        /// </summary>
        public override List<Resolution> SupportedResolutions()
        {
            return DailyResolution;
        }

        /// <summary>
        /// Specifies the data time zone for this data type. This is useful for custom data types
        /// </summary>
        /// <returns>The <see cref="T:NodaTime.DateTimeZone" /> of this data type</returns>
        public override DateTimeZone DataTimeZone()
        {
            return TimeZones.Chicago;
        }
    }
}
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
using NodaTime;
using ProtoBuf;
using QuantConnect;
using QuantConnect.Data;
namespace QuantConnect.DataSource
{
    /// <summary>
    /// Universe Selection helper class for QuiverQuant Twitter Followers dataset
    /// </summary>
    public class QuiverQuantTwitterFollowersUniverse : BaseData
    {
        /// <summary>
        /// Number of followers of the company's Twitter page on the given date
        /// </summary>
        public int Followers { get; set; }

        /// <summary>
        /// Day-over-day change in company's follower count
        /// </summary>
        public decimal DayPercentChange { get; set; }

        /// <summary>
        /// Week-over-week change in company's follower count
        /// </summary>
        public decimal WeekPercentChange { get; set; }

        /// <summary>
        /// Month-over-month change in company's follower count
        /// </summary>
        public decimal MonthPercentChange { get; set; }

        public override DateTime EndTime
        {
            // define end time as exactly 1 day after Time
            get { return Time + QuantConnect.Time.OneDay; }
            set { Time = value - QuantConnect.Time.OneDay; }
        }
        
        public override SubscriptionDataSource GetSource(SubscriptionDataConfig config, DateTime date, bool isLiveMode)
        {
            return new SubscriptionDataSource(
                Path.Combine(
                    Globals.DataFolder,
                    "alternative",
                    "quiver",
                    "twitter",
                    "universe",
                    $"{date.ToStringInvariant(DateFormat.EightCharacter)}.csv"
                ),
                SubscriptionTransportMedium.LocalFile
            );
        }

        public override BaseData Reader(SubscriptionDataConfig config, string line, DateTime date, bool isLiveMode)
        {
            var csv = line.Split(',');

            return new QuiverQuantTwitterFollowersUniverse
            {
                Followers = Parse.Int(csv[2]),
                DayPercentChange = decimal.Parse(csv[3], NumberStyles.Any, CultureInfo.InvariantCulture),
                WeekPercentChange = decimal.Parse(csv[4], NumberStyles.Any, CultureInfo.InvariantCulture),
                MonthPercentChange = decimal.Parse(csv[5], NumberStyles.Any, CultureInfo.InvariantCulture),

                Symbol = new Symbol(SecurityIdentifier.Parse(csv[0]), csv[1]),
                Time = date.AddDays(-1),
            };
        }
    }
}
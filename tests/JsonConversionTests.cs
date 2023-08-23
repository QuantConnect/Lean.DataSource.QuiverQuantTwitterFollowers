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

using System.Linq;
using Newtonsoft.Json;
using NUnit.Framework;
using QuantConnect.DataProcessing;
using QuantConnect.DataSource;

namespace QuantConnect.DataLibrary.Tests
{
    [TestFixture]
    public class JsonConversionTests
    {
        private readonly JsonSerializerSettings _jsonSerializerSettings = new()
        {
            DateTimeZoneHandling = DateTimeZoneHandling.Utc
        };

        [Test]
        public void DeserializeObject()
        {
            var content = @"{
                ""Date"": ""2020-01-01"",
                ""Follower"": 1000,
                ""pct_change_daily"": 5,
                ""pct_change_week"": 100,
                ""pct_change"": 10000
            }";
            var data = JsonConvert.DeserializeObject<QuiverTwitterFollowers>(content,
                                _jsonSerializerSettings);
            var result = QuiverTwitterFollowersDataDownloader.ParseInfo(data);
            var expected = QuiverTwitterFollowersTests.CreateNewInstance();
            
            AssertAreEqual(expected, result);
        }

        [Test]
        public void DeserializeNaN()
        {
            _jsonSerializerSettings.Converters.Add(new NoNanRealConverter());

            var content = @"{
                ""Date"": ""2020-01-01"",
                ""Follower"": 1000,
                ""pct_change_daily"": NaN,
                ""pct_change_week"": NaN,
                ""pct_change"": NaN
            }";
            var data = JsonConvert.DeserializeObject<QuiverTwitterFollowers>(content,
                                _jsonSerializerSettings);

            Assert.IsNull(data.DayPercentChange);
            Assert.IsNull(data.WeekPercentChange);
            Assert.IsNull(data.MonthPercentChange);

            var result = QuiverTwitterFollowersDataDownloader.ParseInfo(data);
            var expected = $"1000,,,";
            
            AssertAreEqual(expected, result);
        }

        private void AssertAreEqual(object expected, object result, bool filterByCustomAttributes = false)
        {
            foreach (var propertyInfo in expected.GetType().GetProperties())
            {
                // we skip Symbol which isn't protobuffed
                if (filterByCustomAttributes && propertyInfo.CustomAttributes.Count() != 0)
                {
                    Assert.AreEqual(propertyInfo.GetValue(expected), propertyInfo.GetValue(result));
                }
            }
            foreach (var fieldInfo in expected.GetType().GetFields())
            {
                Assert.AreEqual(fieldInfo.GetValue(expected), fieldInfo.GetValue(result));
            }
        }
    }
}
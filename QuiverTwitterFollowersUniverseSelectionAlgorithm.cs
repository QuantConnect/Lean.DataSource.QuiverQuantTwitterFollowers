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
using System.Linq;
using QuantConnect.Data;
using QuantConnect.Data.UniverseSelection;
using QuantConnect.DataSource;
using QuantConnect.Securities;

namespace QuantConnect.Algorithm.CSharp
{
    public class QuiverQuantTwitterFollowerUniverseAlgorithm : QCAlgorithm
    {
        private readonly Symbol _symbol = QuantConnect.Symbol.Create("AAPL", SecurityType.Equity, Market.USA);
        private Security _quiverTwitterFollowers;
        private QuiverTwitterFollowersUniverse _datum;

        public override void Initialize()
        {
            // Data ADDED via universe selection is added with Daily resolution.
            UniverseSettings.Resolution = Resolution.Daily;

	        SetStartDate(2022, 2, 14);
            SetEndDate(2022, 2, 18);
            SetCash(100000);

            // Add data for a single security
            _quiverTwitterFollowers = AddData<QuiverTwitterFollowers>(_symbol);

            // add a custom universe data source
            AddUniverse<QuiverTwitterFollowersUniverse>("QuiverTwitterFollowersUniverse", Resolution.Daily, UniverseSelectionMethod);
        }

        private IEnumerable<Symbol> UniverseSelectionMethod(IEnumerable<QuiverTwitterFollowersUniverse> data)
        {
            _datum = data.FirstOrDefault(datum => datum.Symbol == _symbol);

            foreach (var datum in data)
            {
                Log($"{datum.Symbol},{datum.Followers},{datum.DayPercentChange},{datum.WeekPercentChange}");
            }

            return Universe.Unchanged;
        }

        public override void OnData(Slice slice)
        {
            // Sanity check for Universe Selection. The Value (Followers) should be the same.
            // and if-condition should not be true
            var single = _quiverTwitterFollowers?.GetLastData() as QuiverTwitterFollowers;
            if (single?.EndTime == _datum?.EndTime && single?.Followers != _datum?.Followers)
            {
                var message = $"Data mismatch: Single: ({single?.EndTime} > {single?.Followers}) vs Universe ({_datum?.EndTime} > {_datum?.Followers})";
                throw new Exception(message: message);
            }
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace IoTDataProcessor
{
    public class IoTMessage
    {
        public string? Id { get; set; }
        public string? Type { get; set; }
        public JsonElement? Data { get; set; }
    }

    public class DailyReading
    {
        public long? TimeStamp { get; set; }
        public double? Port1 { get; set; }
        public long? ReportCycle { get; set; }
    }

    public class IntervalFlow
    {
        public long? StartTimeStamp { get; set; }
        public int? Interval { get; set; }
        public int Port { get; set; }
        public List<double>? IntervalConsumption { get; set; }
    }

    public class MeterInfo
    {
        public string? Sn { get; set; }
        public string? Imei { get; set; }
        public string? FirmwareVersion { get; set; }
        public string? MeterModel { get; set; }
        public long? TimeStamp { get; set; }
        public int? BattPercentage { get; set; }
        public double? Dot { get; set; }
    }

    public class ApiOutput
    {
        public Header Header { get; set; } = new Header();
        public Payload Payload { get; set; } = new Payload();
    }

    public class Header
    {
        public string? Msn { get; set; }
        public string Type { get; set; } = "W";
    }

    public class Payload
    {
        public List<PayloadData> Data { get; set; } = new List<PayloadData>();
    }

    public class PayloadData
    {
        public string? Dt { get; set; }
        public string? Val { get; set; }
    }

    public static class IoTProcessor
    {
        // Convert Unix timestamp to Singapore local time
        private static DateTime UnixToSGT(long unixSeconds)
        {
            var dt = DateTimeOffset.FromUnixTimeSeconds(unixSeconds).UtcDateTime;
            return dt.AddHours(8); // SGT is UTC+8
        }

        public static List<ApiOutput> ProcessMessages(List<JsonElement> messages)
        {
            var outputs = new List<ApiOutput>();

            // Keep track of previous daily reading per device
            var deviceDailyReading = new Dictionary<string, double>();

            string? msn = null;

            foreach (var message in messages)
            {
                string type = message.GetProperty("type").GetString()!;
                string id = message.GetProperty("id").GetString()!;
                var dataArray = message.GetProperty("data").EnumerateArray();

                switch (type)
                {
                    case "meterInfo":
                        var meterInfoElement = dataArray.First();
                        msn = meterInfoElement.GetProperty("sn").GetString();
                        // We can ignore rest unless needed
                        break;

                    case "dailyReading":
                        var dailyElement = dataArray.First();
                        double dailyValue = dailyElement.GetProperty("port1").GetDouble();
                        deviceDailyReading[id] = dailyValue;

                        // create output
                        if (msn != null)
                        {
                            var output = new ApiOutput
                            {
                                Header = new Header { Msn = msn },
                                Payload = new Payload
                                {
                                    Data = new List<PayloadData>
                                    {
                                        new PayloadData
                                        {
                                            Dt = UnixToSGT(dailyElement.GetProperty("timeStamp").GetInt64()).ToString("yyyy-MM-dd HH:mm:ss"),
                                            Val = dailyValue.ToString("F3")
                                        }
                                    }
                                }
                            };
                            outputs.Add(output);
                        }
                        break;

                    case "intervalFlow":
                        foreach (var intervalElement in dataArray)
                        {
                            if (!deviceDailyReading.ContainsKey(id)) deviceDailyReading[id] = 0;
                            double previous = deviceDailyReading[id];

                            var intervalConsumptionArray = intervalElement.GetProperty("intervalConsumption").EnumerateArray()
                                                        .Select(x => x.GetDouble() / 0.01) // convert to m3
                                                        .ToList();

                            var startTime = intervalElement.GetProperty("startTimeStamp").GetInt64();
                            int interval = intervalElement.GetProperty("interval").GetInt32();

                            for (int i = 0; i < intervalConsumptionArray.Count; i++)
                            {
                                previous += intervalConsumptionArray[i];
                                deviceDailyReading[id] = previous;

                                if (msn != null)
                                {
                                    var dt = UnixToSGT(startTime + i * interval).ToString("yyyy-MM-dd HH:mm:ss");
                                    var output = new ApiOutput
                                    {
                                        Header = new Header { Msn = msn },
                                        Payload = new Payload
                                        {
                                            Data = new List<PayloadData>
                                            {
                                                new PayloadData
                                                {
                                                    Dt = dt,
                                                    Val = previous.ToString("F3")
                                                }
                                            }
                                        }
                                    };
                                    outputs.Add(output);
                                }
                            }
                        }
                        break;

                    case "alarm":
                        // handle alarms if needed
                        break;
                }
            }

            return outputs;
        }
    }
}

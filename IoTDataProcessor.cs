using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;

public class IoTDataProcessor
{
    private readonly HttpClient _httpClient;
    private readonly ILogger _logger;

    public IoTDataProcessor(ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<IoTDataProcessor>();
        _httpClient = new HttpClient();
    }

    [Function("ProcessIoTHubData")]
    public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Function, "post")] HttpRequestData req)
    {
        string requestBody = await new StreamReader(req.Body).ReadToEndAsync();

        _logger.LogInformation("Received data: {data}", requestBody);

        // Parse incoming JSON
        var incomingData = JsonSerializer.Deserialize<IncomingData>(requestBody, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

        if (incomingData == null)
        {
            var badResponse = req.CreateResponse(System.Net.HttpStatusCode.BadRequest);
            await badResponse.WriteStringAsync("Invalid input data");
            return badResponse;
        }

        // Call processing logic here
        var result = await ProcessDataAsync(incomingData);

        // Return OK response (or error as needed)
        var response = req.CreateResponse(System.Net.HttpStatusCode.OK);
        await response.WriteStringAsync("Processed successfully");
        return response;
    }

    private async Task ProcessDataAsync(IncomingData incomingData)
    {
        // In a real scenario, you need to maintain state (previous daily reading values) per device.
        // For demo, let's keep this simple with an in-memory dictionary (reset per run).
        // For production, use persistent storage (database, Redis, etc.).

        // Simulate state - for demo, a static dictionary (in production, replace this)
        var state = StateHolder.GetState(incomingData.Id);

        switch (incomingData.Type)
        {
            case "dailyReading":
                // Update daily reading in state
                var dailyReadingData = incomingData.Data?[0];
                if (dailyReadingData != null && dailyReadingData.ReportCycle > 0)
                {
                    state.DailyReading = dailyReadingData.Port1;
                    state.DailyReadingTimestamp = UnixTimeStampToDateTime(dailyReadingData.TimeStamp);
                }
                break;

            case "intervalFlow":
                // For each interval, calculate cumulative reading
                var intervalData = incomingData.Data?[0];
                if (intervalData != null)
                {
                    // intervalConsumption is an array of integers, each represents count * 0.01 to convert to m3
                    // Add to previous dailyReading cumulative
                    var baseReading = state.DailyReading;

                    // Build output data array
                    var outputData = new List<OutputDataPoint>();

                    DateTime baseTimestamp = UnixTimeStampToDateTime(intervalData.StartTimeStamp);
                    int intervalSeconds = intervalData.Interval;

                    for (int i = 0; i < intervalData.IntervalConsumption.Length; i++)
                    {
                        double m3 = intervalData.IntervalConsumption[i] / 0.01; // Convert to m3
                        double cumulative = baseReading + m3;

                        var timestamp = baseTimestamp.AddSeconds(i * intervalSeconds);

                        // Convert to Singapore Time (UTC+8)
                        var sgTime = TimeZoneInfo.ConvertTimeBySystemTimeZoneId(timestamp, "UTC", "Singapore Standard Time");

                        outputData.Add(new OutputDataPoint
                        {
                            Dt = sgTime.ToString("yyyy-MM-dd HH:mm:ss"),
                            Val = cumulative.ToString("F3", CultureInfo.InvariantCulture)
                        });
                    }

                    // Prepare payload
                    var payload = new List<OutputPayload>
                    {
                        new OutputPayload
                        {
                            Header = new OutputHeader
                            {
                                Msn = state.MeterInfo?.Sn ?? incomingData.Id,
                                Type = "W"
                            },
                            Payload = new PayloadData
                            {
                                Data = outputData
                            }
                        }
                    };

                    // Serialize to JSON to send to API
                    string jsonPayload = JsonSerializer.Serialize(payload);

                    _logger.LogInformation("Prepared JSON to send: {json}", jsonPayload);

                    // TODO: Send this payload to API endpoint
                    // await _httpClient.PostAsync("https://your-api-endpoint", new StringContent(jsonPayload, Encoding.UTF8, "application/json"));
                }
                break;

            case "meterInfo":
                var meterInfo = incomingData.Data?[0];
                if (meterInfo != null)
                {
                    state.MeterInfo = new MeterInfo
                    {
                        Sn = meterInfo.Sn,
                        Imei = meterInfo.Imei,
                        FirmwareVersion = meterInfo.FirmwareVersion,
                        BattPercentage = meterInfo.BattPercentage
                    };
                }
                break;

            case "alarm":
                // You can handle alarms here if needed
                break;

            default:
                _logger.LogWarning("Unknown data type: {type}", incomingData.Type);
                break;
        }

        // Save back state if persistent (omitted here)
    }

    // Helper to convert Unix timestamp (seconds) to DateTime UTC
    private DateTime UnixTimeStampToDateTime(long unixTimeStamp)
    {
        DateTime epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        return epoch.AddSeconds(unixTimeStamp);
    }
}

#region Helper Classes & State

// Simple in-memory state holder - demo only!
public static class StateHolder
{
    private static readonly Dictionary<string, DeviceState> _states = new();

    public static DeviceState GetState(string deviceId)
    {
        if (!_states.ContainsKey(deviceId))
            _states[deviceId] = new DeviceState();

        return _states[deviceId];
    }
}

public class DeviceState
{
    public double DailyReading { get; set; } = 0;
    public DateTime DailyReadingTimestamp { get; set; } = DateTime.MinValue;
    public MeterInfo MeterInfo { get; set; }
}

public class MeterInfo
{
    public string Sn { get; set; }
    public string Imei { get; set; }
    public string FirmwareVersion { get; set; }
    public int BattPercentage { get; set; }
}

public class IncomingData
{
    [JsonPropertyName("id")]
    public string Id { get; set; }

    [JsonPropertyName("type")]
    public string Type { get; set; }

    [JsonPropertyName("data")]
    public JsonElement[] Data { get; set; }
}

public class DailyReadingData
{
    [JsonPropertyName("reportCycle")]
    public int ReportCycle { get; set; }

    [JsonPropertyName("timeStamp")]
    public long TimeStamp { get; set; }

    [JsonPropertyName("port1")]
    public double Port1 { get; set; }
}

public class IntervalFlowData
{
    [JsonPropertyName("interval")]
    public int Interval { get; set; }

    [JsonPropertyName("startTimeStamp")]
    public long StartTimeStamp { get; set; }

    [JsonPropertyName("port")]
    public int Port { get; set; }

    [JsonPropertyName("intervalConsumption")]
    public int[] IntervalConsumption { get; set; }
}

// Output classes to match your expected output JSON

public class OutputPayload
{
    [JsonPropertyName("header")]
    public OutputHeader Header { get; set; }

    [JsonPropertyName("payload")]
    public PayloadData Payload { get; set; }
}

public class OutputHeader
{
    [JsonPropertyName("msn")]
    public string Msn { get; set; }

    [JsonPropertyName("type")]
    public string Type { get; set; }
}

public class PayloadData
{
    [JsonPropertyName("data")]
    public List<OutputDataPoint> Data { get; set; }
}

public class OutputDataPoint
{
    [JsonPropertyName("dt")]
    public string Dt { get; set; }

    [JsonPropertyName("val")]
    public string Val { get; set; }
}

#endregion

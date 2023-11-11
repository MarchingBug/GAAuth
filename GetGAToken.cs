using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using Google.Analytics.Data.V1Beta;
using Google.Apis.Auth.OAuth2;
using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Identity.Client.Platforms.Features.DesktopOs.Kerberos;
using System.Collections.Generic;
using System.Net;
using Grpc.Auth;
using Grpc.Core;
using System.IO;
using Newtonsoft.Json;
using Azure.Storage.Blobs;
using System.Text;
using Google.Api;
using Microsoft.Extensions.Configuration;

namespace GAAuth
{
    public  class GetGAToken
    {
             

        private string GetMetrics(ILogger log, GoogleCredential cred, string propertyId, string dimension= "city", string metric= "activeUsers", string start_date= "2020-03-31", string end_date= "today")
        {
            try
            {


                BetaAnalyticsDataClient client = new BetaAnalyticsDataClientBuilder
                {
                    Credential = cred
                }.Build();

                RunReportRequest request = new RunReportRequest
                {
                    Property = "properties/" + propertyId,
                    Dimensions = { new Dimension { Name = dimension }, },
                    Metrics = { new Google.Analytics.Data.V1Beta.Metric { Name = metric }, },
                    DateRanges = { new DateRange { StartDate = start_date, EndDate = end_date }, },
                };

                var response = client.RunReport(request);
                string jsonResponse = Newtonsoft.Json.JsonConvert.SerializeObject(response);
                var jsonObject = Newtonsoft.Json.JsonConvert.DeserializeObject<dynamic>(jsonResponse);
                var rows = jsonObject.Rows;
                List<dynamic> parsedRows = new();
                foreach (var row in rows)
                {
                    var dimensionValues = row.DimensionValues;
                    var metricValues = row.MetricValues;
                    foreach (var dimensionValue in dimensionValues)
                    {
                        foreach (var metricValue in metricValues)
                        {
                            var parsedRow = new { propertyId = propertyId, dimension = dimensionValue.Value, metric = metricValue.Value, start_date = start_date, end_date = end_date };
                            parsedRows.Add(parsedRow);
                        }
                    }
                }
                string parsedJson = Newtonsoft.Json.JsonConvert.SerializeObject(parsedRows);

                
                return parsedJson;
            }
            catch (Exception ex)
            {
                log.LogError("error on GetMetrics" + ex.Message + " " + ex.StackTrace);
                return "";
            }
        }

        public static string RandomString(int size)
            {
                StringBuilder builder = new StringBuilder();
                Random random = new Random();
                char ch;
                for (int i = 0; i < size; i++)
                {
                    ch = Convert.ToChar(Convert.ToInt32(Math.Floor(26 * random.NextDouble() + 65)));                 
                    builder.Append(ch);   
                }
                return builder.ToString();
            }


        public static bool IsDate(string input)
            {
             return DateTime.TryParse(input, out DateTime temp);
            }

        public static string FormatDate(string input)
        {
            DateTime.TryParse(s: input, out DateTime temp);
            return temp.ToString("yyyy-MM-dd");
        }

        [FunctionName("GetGoogleAnaltycisData")]       
        public async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get","post", Route = null)] HttpRequest req, ILogger log)            
        {
            try{
                log.LogInformation("C# HTTP trigger function processed a request.");

                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                dynamic data = JsonConvert.DeserializeObject(requestBody);
                string propertyId =  data.propertyId;
                string dimension = data.dimension;
                string metric = data.metric;
                string start_date = data.start_date;
                string end_date = data.end_date;

                if (IsDate(start_date))
                {
                    start_date = FormatDate(start_date);
                }
                else
                {
                    return new BadRequestObjectResult("start_date is not a valid date");
                }

                 if (IsDate(end_date))
                {
                    end_date = FormatDate(end_date);
                }
                else
                {
                    return new BadRequestObjectResult("end_date is not a valid date");
                }

                string storageAccountConnection = Environment.GetEnvironmentVariable("DataLakeConnectionString");
                var kvClient = new SecretClient(new Uri(Environment.GetEnvironmentVariable("KEY_VAULT_URL")), new ManagedIdentityCredential());
                log.LogInformation("got key vault client");

                string keyJson = kvClient.GetSecret("GAAuthToken").Value.Value;
                log.LogInformation("got keyjson");                

                var cred = GoogleCredential.FromJson(keyJson).CreateScoped(new string[] { "https://www.googleapis.com/auth/analytics.readonly", "https://www.googleapis.com/auth/analytics", "https://www.googleapis.com/auth/plus.login" });
                

                log.LogInformation("got credentials");
                var token = await cred.UnderlyingCredential.GetAccessTokenForRequestAsync();
               

                string result = GetMetrics(log, cred, propertyId, dimension, metric, start_date, end_date);

               
                BlobContainerClient outputBlob = new BlobContainerClient(storageAccountConnection, "google");


                string dateFormat = $"{RandomString(4)}_{DateTime.Now.Year}_{DateTime.Now.Month}_{DateTime.Now.Day}_{DateTime.Now.Hour}_{DateTime.Now.Minute}";
                string blobName = $"/RawJsonNotProccessed/GoogleAnalytics_{dateFormat}.json";

                BlobClient blob = outputBlob.GetBlobClient(blobName);              

                try
                {

                    using (MemoryStream ms = new MemoryStream(Encoding.UTF8.GetBytes(result)))
                    {
                        await blob.UploadAsync(ms);
                    }

                }
                catch (Exception ex)
                {
                    log.LogError(ex.Message);
                }


                //return new OkObjectResult("{\"result\":\"" + result + "\"}");
                return new OkObjectResult("Request processed, file " + blobName + "saved ");
            }
            catch(Exception ex)
            {
                log.LogError(ex.Message);
                return new BadRequestObjectResult(ex.Message);
            }
        }
    }
}

/* Copyright 2021 Esri
 *
 * Licensed under the Apache License Version 2.0 (the "License");
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

using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
//using System.Globalization;
//using System.IO;
using System.Linq;
//using System.Net;
using System.Net.Http;
//using System.Net.Http.Headers;
//using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Grpc.Core;
using Grpc.Net.Client;
using Google.Protobuf.WellKnownTypes;
using Esri.Realtime.Core.Grpc;

namespace gRPC_NYC_Sender
{
    class Program
    {
        
        private static string gRPC_endpoint_URL = ConfigurationManager.AppSettings["gRPC_endpoint_URL"];
        private static string gRPC_endpoint_header_path = ConfigurationManager.AppSettings["gRPC_endpoint_header_path"];
        private static bool streamData = Boolean.Parse(ConfigurationManager.AppSettings["streamData"]);
        private static bool authenticationArcGIS = Boolean.Parse(ConfigurationManager.AppSettings["authenticationArcGIS"]);
        private static string tokenPortalUrl = ConfigurationManager.AppSettings["tokenPortalUrl"];
        private static string username = ConfigurationManager.AppSettings["username"];
        private static string password = ConfigurationManager.AppSettings["password"];
        private static int tokenExpiry = Int32.Parse(ConfigurationManager.AppSettings["tokenExpiry"]);
        private static string baseUrl = ConfigurationManager.AppSettings["baseUrl"];
        private static string apiKey = ConfigurationManager.AppSettings["apiKey"];
        private static string[] lineRefs = ConfigurationManager.AppSettings["lineRefs"].Split(',');
        private static int sendInterval = Int32.Parse(ConfigurationManager.AppSettings["sendInterval"]);

        private static readonly HttpClient httpClient = new HttpClient();
        
        static async Task Main()
        {
            Console.WriteLine("Starting...");
            
            try
            {      

                ////// gRPC feed connection parameters
                //gRPC_endpoint_URL = "";
                //gRPC_endpoint_header_path = "";
                
                string token = "";                
                bool runTask = true;


                var handler = new System.Net.Http.SocketsHttpHandler
                {
                    PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan
                //    KeepAlivePingDelay = TimeSpan.FromSeconds(20),
                //    KeepAlivePingTimeout = TimeSpan.FromSeconds(20),
                //    EnableMultipleHttp2Connections = true
                };
                

                //using var channel = GrpcChannel.ForAddress(String.Format("https://{0}:443", gRPC_endpoint_URL), new GrpcChannelOptions
                //{
                //    HttpHandler = handler
                //});

                using var channel = GrpcChannel.ForAddress($"https://{gRPC_endpoint_URL}:443");
                var grpcClient = new GrpcFeed.GrpcFeedClient(channel);   

                var metadata = new Grpc.Core.Metadata
                {
                    { "grpc-path", gRPC_endpoint_header_path }
                };

                                
                if (authenticationArcGIS){
                    string tokenStr = await getToken(tokenPortalUrl,username,password,tokenExpiry);                     
                    if (tokenStr.Contains("Unable to generate token.")){
                        Console.WriteLine(tokenStr);
                        return;
                    }                 
                    dynamic tokenJson = JsonConvert.DeserializeObject(tokenStr); 
                    token = tokenJson["token"];               
                    metadata.Add("authorization", $"Bearer {token}");                    
                }

                AsyncClientStreamingCall<Request,Response> call = streamData ? grpcClient.Stream(metadata) : null;                          
                              
                
                int featuresInBatchCount = 0;
                int totalFeaturesSentCount = 0;
                Request request = new Request();
                Response response = new Response();
                        

                var stopwatch = new Stopwatch();
                var taskStopwatch = new Stopwatch();
                taskStopwatch.Start();
                while (runTask)
                {
                    
                    if (request.Features.Count == 0)                        
                        stopwatch.Start();
                    int tick_time = (int)taskStopwatch.ElapsedMilliseconds;
                    //Console.WriteLine("Fetching data...");
                    JArray contentArray = await fetchData();
                    //Console.WriteLine($"Data received in {(int)taskStopwatch.ElapsedMilliseconds - tick_time}ms");
                    int lineCount = contentArray.Count;
                    foreach (JToken vmd in contentArray)
                    {                           
                        JToken vaArray = vmd["VehicleActivity"];
                        if (vaArray == null)
                            continue;
                        foreach (JToken va in vaArray)  {                   
                            Feature feature = new Feature();
                            JToken mvj = va["MonitoredVehicleJourney"];
                            
                            string lr = "";
                            string dr = "";
                            string dfr = "";
                            string dvjr = "";
                            string jpr = "";
                            string pln = "";
                            string opr = "";
                            string orr = "";
                            string der = "";
                            string dn = "";
                            string oadt = "";
                            //bool m = (bool)mvj["Monitored"];
                            double lon = 0.0;
                            double lat = 0.0;
                            double b = 0.0;
                            string pr = "";
                            string ps = "";
                            string br = "";
                            string vr = "";
                            string aat = "";
                            string eat = "";
                            string apt = "";
                            string edt = "";
                            int dfs = -1;
                            int nosa = -1;
                            string spr = "";
                            int vn = -1;
                            string spn = "";
                            string rat = "";

                            try{
                                try{lr = (string)mvj["LineRef"];}catch(Exception e){}
                                try{dr = (string)mvj["DirectionRef"];}catch(Exception e){}
                                try{dfr = (string)mvj["FramedVehicleJourneyRef"]["DataFrameRef"];}catch(Exception e){}
                                try{dvjr = (string)mvj["FramedVehicleJourneyRef"]["DatedVehicleJourneyRef"];}catch(Exception e){}
                                try{jpr = (string)mvj["JourneyPatternRef"];}catch(Exception e){}
                                try{pln = (string)mvj["PublishedLineName"][0];}catch(Exception e){}
                                try{opr = (string)mvj["OperatorRef"];}catch(Exception e){}
                                try{orr = (string)mvj["OriginRef"];}catch(Exception e){}
                                try{der = (string)mvj["DestinationRef"];}catch(Exception e){}
                                try{dn = mvj["DestinationName"][0] != null ? (string)mvj["DestinationName"][0] : "";}catch(Exception e){}
                                try{oadt = mvj["OriginAimedDepartureTime"] != null ? (string)mvj["OriginAimedDepartureTime"] : "";}catch(Exception e){}
                                try{lon = (double)mvj["VehicleLocation"]["Longitude"];}catch(Exception e){}
                                try{lat = (double)mvj["VehicleLocation"]["Latitude"];}catch(Exception e){}
                                try{b = mvj["Bearing"] != null ? (double)mvj["Bearing"] : 0.0;}catch(Exception e){}
                                try{ps = mvj["ProgressStatus"] != null ? (string)mvj["ProgressStatus"][0] : "";}catch(Exception e){}
                                try{pr = mvj["ProgressRate"] != null ? (string)mvj["ProgressRate"] : "";}catch(Exception e){}
                                try{br = mvj["BlockRef"] != null ? (string)mvj["BlockRef"] : "";}catch(Exception e){}
                                try{vr = (string)mvj["VehicleRef"];}catch(Exception e){}
                                try{aat = mvj["MonitoredCall"]["AimedArrivalTime"] != null ? (string)mvj["MonitoredCall"]["AimedArrivalTime"] : "";}catch(Exception e){}
                                try{eat = mvj["MonitoredCall"]["ExpectedArrivalTime"] != null ? (string)mvj["MonitoredCall"]["ExpectedArrivalTime"] : "";}catch(Exception e){}
                                try{apt = mvj["MonitoredCall"]["ArrivalProximityText"] != null ? (string)mvj["MonitoredCall"]["ArrivalProximityText"] : "";}catch(Exception e){}
                                try{edt = mvj["MonitoredCall"]["ExpectedDepartureTime"] != null ? (string)mvj["MonitoredCall"]["ExpectedDepartureTime"] : "";}catch(Exception e){}
                                try{dfs = mvj["MonitoredCall"]["DistanceFromStop"] != null ? (int)mvj["MonitoredCall"]["DistanceFromStop"] : -1;}catch(Exception e){dfs = -1;}
                                try{nosa = mvj["MonitoredCall"]["NumberOfStopsAway"] != null ? (int)mvj["MonitoredCall"]["NumberOfStopsAway"] : -1;}catch(Exception e){nosa = -1;}
                                try{spr = mvj["MonitoredCall"]["StopPointRef"] != null ? (string)mvj["MonitoredCall"]["StopPointRef"] : "";}catch(Exception e){}
                                try{spn = mvj["MonitoredCall"]["StopPointName"][0] != null ? (string)mvj["MonitoredCall"]["StopPointName"][0] : "";}catch(Exception e){}
                                try{vn = mvj["MonitoredCall"]["VisitNumber"] != null ? (int)mvj["MonitoredCall"]["VisitNumber"] : -1;}catch(Exception e){vn = -1;}
                                try{rat = va["RecordedAtTime"] != null ? (string)va["RecordedAtTime"] : "";}catch(Exception e){}

                                feature.Attributes.Add(Any.Pack(new StringValue() { Value = lr }));
                                feature.Attributes.Add(Any.Pack(new StringValue() { Value = dr }));
                                feature.Attributes.Add(Any.Pack(new StringValue() { Value = dfr }));
                                feature.Attributes.Add(Any.Pack(new StringValue() { Value = dvjr }));
                                feature.Attributes.Add(Any.Pack(new StringValue() { Value = jpr }));
                                feature.Attributes.Add(Any.Pack(new StringValue() { Value = pln }));
                                feature.Attributes.Add(Any.Pack(new StringValue() { Value = opr }));
                                feature.Attributes.Add(Any.Pack(new StringValue() { Value = orr }));
                                feature.Attributes.Add(Any.Pack(new StringValue() { Value = der }));
                                feature.Attributes.Add(Any.Pack(new StringValue() { Value = dn }));
                                feature.Attributes.Add(Any.Pack(new StringValue() { Value = oadt }));
                                feature.Attributes.Add(Any.Pack(new DoubleValue() { Value = lon }));
                                feature.Attributes.Add(Any.Pack(new DoubleValue() { Value = lat })); 
                                feature.Attributes.Add(Any.Pack(new DoubleValue() { Value = b })); 
                                feature.Attributes.Add(Any.Pack(new StringValue() { Value = pr }));
                                feature.Attributes.Add(Any.Pack(new StringValue() { Value = ps }));  
                                feature.Attributes.Add(Any.Pack(new StringValue() { Value = br })); 
                                feature.Attributes.Add(Any.Pack(new StringValue() { Value = vr })); 
                                feature.Attributes.Add(Any.Pack(new StringValue() { Value = aat })); 
                                feature.Attributes.Add(Any.Pack(new StringValue() { Value = eat })); 
                                feature.Attributes.Add(Any.Pack(new StringValue() { Value = apt }));
                                feature.Attributes.Add(Any.Pack(new StringValue() { Value = edt }));    
                                feature.Attributes.Add(Any.Pack(new Int32Value() { Value = dfs })); 
                                feature.Attributes.Add(Any.Pack(new Int32Value() { Value = nosa })); 
                                feature.Attributes.Add(Any.Pack(new StringValue() { Value = spr })); 
                                feature.Attributes.Add(Any.Pack(new Int32Value() { Value = vn })); 
                                feature.Attributes.Add(Any.Pack(new StringValue() { Value = spn })); 
                                feature.Attributes.Add(Any.Pack(new StringValue() { Value = rat }));                         

                                request.Features.Add(feature);                  
                            }
                            catch(Exception ex){
                                Console.WriteLine(ex.Message);
                                Console.WriteLine(ex.StackTrace);
                                Console.WriteLine(ex.Data);
                                continue;
                            }    
                            featuresInBatchCount++;
                            totalFeaturesSentCount++;
                        }
                        
                    }

                    
                    //Console.WriteLine($"Sending data. Fetching and packing data took {(int)taskStopwatch.ElapsedMilliseconds - tick_time}ms");
                        
                    
                    // send the batch of events to the gRPC receiver                           
                    //if the request fails because the token expired, get a new one and retry the request
                    try{
                        string responseString;
                        if (!streamData){
                            response = await grpcClient.SendAsync(request, metadata);
                            responseString = response.Message;
                            //Console.WriteLine($"gRPC feed response: {responseString}");
                            if (response.Code == 7 && authenticationArcGIS){                                    
                                Console.WriteLine($"Renewing the token for {username}");
                                string tokenStr = await getToken(tokenPortalUrl,username,password,tokenExpiry);                     
                                if (tokenStr.Contains("Unable to generate token.")){
                                    Console.WriteLine(tokenStr);
                                    return;
                                }                 
                                dynamic tokenJson = JsonConvert.DeserializeObject(tokenStr); 
                                token = tokenJson["token"];                                    
                                metadata[1] = new Grpc.Core.Metadata.Entry("authorization", $"Bearer {token}");
                                response = await grpcClient.SendAsync(request, metadata); 
                                responseString = response.Message;   
                            }
                            Console.WriteLine(string.Format($"A batch of {featuresInBatchCount} events was sent with response from the server: {responseString}. Total sent: {totalFeaturesSentCount}. Total elapsed time: {(int)taskStopwatch.ElapsedMilliseconds}ms"));        
                            
                        }
                        else{
                            //using var caller = grpcClient.Stream(metadata);
                            await call.RequestStream.WriteAsync(request);
                            Console.WriteLine(string.Format($"A batch of {featuresInBatchCount} events was streamed to the gRPC server . Total sent: {totalFeaturesSentCount}. Total elapsed time: {(int)taskStopwatch.ElapsedMilliseconds}ms"));        
                            //await call.RequestStream.CompleteAsync();
                        }
                                              
                        
                        stopwatch.Stop();
                        int elapsed_time = (int)stopwatch.ElapsedMilliseconds;                                
                        if (elapsed_time < sendInterval) {
                            //Console.WriteLine($"Sleeping for about {sendInterval - ((int)taskStopwatch.ElapsedMilliseconds - tick_time)}ms");
                            Thread.Sleep(sendInterval - elapsed_time);
                        }
                    }
                    catch (Exception ex){
                        Console.WriteLine(string.Format($"A batch of {featuresInBatchCount} events was sent, but the request failed. Total sent: {totalFeaturesSentCount}. Total elapsed time: {(int)taskStopwatch.ElapsedMilliseconds}ms"));
                        Console.WriteLine(ex.Message);
                    }
                    finally{
                        request.Features.Clear();
                        stopwatch.Reset();
                        featuresInBatchCount = 0;
                    } 
                }
                
                if (streamData){
                    await call.RequestStream.CompleteAsync();
                    //var response = await call;
                    //var responseString = response.Message;  
                }
 
                
                
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
                Console.WriteLine(e.Data);
            }
        }

        static async Task<string> getToken(string url, string user, string pass, double expiry)
        {    
            Console.WriteLine("Fetching a new token.");
            try
            {        
                var values = new Dictionary<string, string>
                {
                    { "username", user },
                    { "password", pass },
                    { "client", "referer" },
                    { "referer", "http://localhost:8888"},
                    { "f", "json"},
                    { "expiration", expiry.ToString()}
                };
                
                var content = new FormUrlEncodedContent(values);
                var response = await httpClient.PostAsync($"{url}/sharing/rest/generateToken", content);   
                if (response.IsSuccessStatusCode)  {       
                    var responseString = await response.Content.ReadAsStringAsync();
                    return responseString;
                }
                else
                {
                    return $"Unable to generate token. {response.ReasonPhrase}";
                }
            }
            catch (Exception e)
            {
                Console.Out.WriteLine("getToken Error: " + e.Message);
                //log.LogInformation("Error: " + e.Message);
                return "getToken Error: " + e.Message;
            }
        }
    
        static async Task<JArray> fetchData(){
              
            HttpClient httpClient = new HttpClient();
            httpClient.DefaultRequestHeaders.TryAddWithoutValidation("Accept", "*/*");
            httpClient.DefaultRequestHeaders.TryAddWithoutValidation("Client", "Referer");
            httpClient.DefaultRequestHeaders.TryAddWithoutValidation("Referer", "http://localhost:8888");
            httpClient.DefaultRequestHeaders.TryAddWithoutValidation("Content-Type", "application/json; charset=utf-8");

            JArray returnArray = new JArray();
            try{
                foreach (string lineRef in lineRefs){  
                    string reqUrl = lineRef == ""? $"{baseUrl}&key={apiKey}" : $"{baseUrl}&key={apiKey}&LineRef={lineRef.Trim()}";
                    

                    var response = httpClient.GetAsync(reqUrl).Result;
                    var responseString = await response.Content.ReadAsStringAsync();
                    dynamic responseJson = JsonConvert.DeserializeObject(responseString);
                    returnArray = new JArray(returnArray.Union(responseJson["Siri"]["ServiceDelivery"]["VehicleMonitoringDelivery"] as JArray));
                } 
            }
            catch(Exception ex){

            }

            return returnArray;

        }
    }
}

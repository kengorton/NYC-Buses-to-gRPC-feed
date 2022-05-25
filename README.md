# NYC-Buses-to-gRPC-feed

This is a C#.Net console application to fetch NYC Buses records from the New York City Buses authority (https://api.prod.obanyc.com/).

You will need a free developer API Key from here: https://api.prod.obanyc.com/wiki/Developers/Index

Place your API Key in the app.config configuration > appSettings section as the value for the apiKey attribute.

Create a gRPC feed in Velocity using the following sample schema:

````
{
    "LineRef": "MTA NYCT_M3",
    "DirectionRef": "1",
    "DataFrameRef": "2022-02-25",
    "DatedVehicleJourneyRef": "MTA NYCT_MV_A2-Weekday-089400_M3_340",
    "JourneyPatternRef": "MTA_M030008",
    "PublishedLineName": "M1",
    "OperatorRef": "MTA NYCT",
    "OriginRef": "MTA_405091",
    "DestinationRef": "MTA_903048",
    "DestinationName": "EAST VILLAGE 8 ST via 5 AV",
    "OriginAimedDepartureTime": "2022-02-16T10:24:00.000-05:00",
    "Longitude": -73.990592,
    "Latitude": 40.73029,
    "Bearing": 73.663956,
    "ProgressStatus":"layover",
    "ProgressRate":"normalProgress",
    "BlockRef": "MTA NYCT_MV_A2-Weekday_C_MV_49140_M3-340",
    "VehicleRef": "MTA NYCT_3958",
    "AimedArrivalTime": "2022-02-16T10:04:00.910-05:00",
    "ExpectedArrivalTime": "2022-02-16T10:09:48.844-05:00",
    "ArrivalProximityText": "at stop",
    "ExpectedDepartureTime": "2022-02-16T10:09:48.844-05:00",
    "DistanceFromStop": 7,
    "NumberOfStopsAway": 0,
    "StopPointRef": "MTA_403979",
    "VisitNumber": 1,
    "StopPointName": "ST NICHOLAS AV/W 174 ST",
    "RecordedAtTime": "2022-02-15T17:09:51.000-05:00"
}
````
Copy the 'gRPC endpoint URL' and 'gRPC endpoint header path' values from the new feed and provision them to the app.config under configuration > appSettings > gRPC_endpoint_URL and gRPC_endpoint_header_path, respectively.
When executed, the code in program.cs will fetch the current bus locations for the bus lines included in configuration > appSettings > lineRefs or for all bus lines if this attribute is empty.
The sample setting of "M116,M106,M104,M103,M102,M101,M100,M98,M96,M86,M79,M72,M66,M60,M57,M55,M50,M42,M35,M34A,M34,M31,M23,M22,M21,M20,M15,M14D,M14A,M12,M11,M10,M9,M7,M8,M5,M4,M3,M2,M1" includes all of the bus lines for Manhattan.

The json response is parsed to extract key attributes and flattened to match the schema above. The extracted attributes are packed into a gRPC Feature object per the velocity_grpc.proto file in the Protos folder.

The Features objects are then sent to the gRPC feed configured above.


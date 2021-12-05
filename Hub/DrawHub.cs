// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.AspNetCore.SignalR;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using System;
using Google.Cloud.Spanner.Data;

namespace Microsoft.Azure.SignalR.Samples.Whiteboard
{
   
    public class DrawHub : Hub
    {
        private Diagram diagram;

        static string projectId = "eighth-veld-333913";
        static string databaseId = "db1";
        static string instanceId = "inst1";
        static string connectionString =
           $"Data Source=projects/{projectId}/instances/{instanceId}/"
            + $"databases/{databaseId}";

        public DrawHub(Diagram diagram)
        {
            this.diagram = diagram;
        }
        public override Task OnConnectedAsync()
        {
            var t = Task.WhenAll(diagram.Shapes.AsEnumerable().Select(l => Clients.Client(Context.ConnectionId).SendAsync("ShapeUpdated", l.Key, l.Value)));
            if (diagram.Background != null) t = t.ContinueWith(_ => Clients.Client(Context.ConnectionId).SendAsync("BackgroundUpdated", diagram.BackgroundId));
            return t.ContinueWith(_ => Clients.All.SendAsync("UserUpdated", diagram.UserEnter()));
        }

        public override Task OnDisconnectedAsync(Exception exception)
        {
            return Clients.All.SendAsync(" ", diagram.UserLeave());
        }

        public async Task PatchShape(string id, List<int> data)
        {
            diagram.Shapes[id].Data.AddRange(data);
            await Clients.Others.SendAsync("ShapePatched", id, data);
            //Create connection to Cloud Spanner.     

            using (var connection = new SpannerConnection(connectionString))
            {             
                var cmd = connection.CreateSelectCommand(
               @"SELECT ShapeInfoId,ShapeKind from ShapesInfo");

                int rowcount = 0, counter = 0;

                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        rowcount++;
                    }
                }

                counter = rowcount;

                await connection.RunWithRetriableTransactionAsync(async transaction =>
                {
                    // Insert rows into the ShapesInfo table.
                    var cmd = connection.CreateInsertCommand("ShapesInfo",
                        new SpannerParameterCollection
                        {
                    { "Id", SpannerDbType.Int64 },
                    { "ShapeInfoId", SpannerDbType.String },
                    { "ShapeKind", SpannerDbType.String },
                    { "ShapePlotPoint", SpannerDbType.Int64}
                        });

                    cmd.Transaction = transaction;

                    await Task.WhenAll(data.Select(x =>
                    {
                        counter = counter + 1;
                        cmd.Parameters["Id"].Value = counter;
                        cmd.Parameters["ShapeInfoId"].Value = id;
                        cmd.Parameters["ShapeKind"].Value = "polyline";
                        cmd.Parameters["ShapePlotPoint"].Value = x;

                        return cmd.ExecuteNonQueryAsync();
                    }));

                });
            }

           
        }

        public async Task UpdateShape(string id, Diagram.Shape shape)
        {
            diagram.Shapes[id] = shape;
            await Clients.Others.SendAsync("ShapeUpdated", id, shape);

            //Create connection to Cloud Spanner.
            using (var connection = new SpannerConnection(connectionString))
            {

                var cmd = connection.CreateSelectCommand(
               @"SELECT ShapeInfoId,ShapeKind from ShapesInfo");

                int rowcount = 0, counter = 0;

                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        rowcount++;
                    }
                }

                counter = rowcount;

                await connection.RunWithRetriableTransactionAsync(async transaction =>
                {
                    // Insert rows into the ShapesInfo table.
                    var cmd = connection.CreateInsertCommand("ShapesInfo",
                        new SpannerParameterCollection
                        {
                    { "Id", SpannerDbType.Int64 },
                    { "ShapeInfoId", SpannerDbType.String },
                    { "ShapeKind", SpannerDbType.String },
                    { "ShapePlotPoint", SpannerDbType.Int64}
                        });

                    cmd.Transaction = transaction;

                    await Task.WhenAll(shape.Data.Select(x =>
                    {
                        counter = counter + 1;
                        cmd.Parameters["Id"].Value = counter;
                        cmd.Parameters["ShapeInfoId"].Value = id;
                        cmd.Parameters["ShapeKind"].Value = shape.Kind;
                        cmd.Parameters["ShapePlotPoint"].Value = x;

                        return cmd.ExecuteNonQueryAsync();
                    }));

                });
            }

            
        }

        public async Task RemoveShape(string id)
        {
            diagram.Shapes.Remove(id, out _);
            await Clients.Others.SendAsync("ShapeRemoved", id);
        }

        public async Task Clear()
        {         
            diagram.Shapes.Clear();
            diagram.Background = null;
            await Clients.Others.SendAsync("Clear");
        }

        public async Task SendMessage(string name, string message)
        {
            await Clients.Others.SendAsync("NewMessage", name, message);

            //Create connection to Cloud Spanner.
            using (var connection = new SpannerConnection(connectionString))
            {

                var cmd = connection.CreateSelectCommand(
               @"SELECT Username, Messages from Chat");

                int rowcount = 0;

                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        rowcount++;
                    }
                }

                await connection.RunWithRetriableTransactionAsync(async transaction =>
                {
                // Insert rows into the Chat table.
                var cmd = connection.CreateInsertCommand("Chat",
                    new SpannerParameterCollection
                    {
                        { "Id", SpannerDbType.Int64 },
                        { "Username", SpannerDbType.String },
                        { "Messages", SpannerDbType.String },
                        { "MessageTime", SpannerDbType.String }

                    });

                cmd.Transaction = transaction;

                cmd.Parameters["Id"].Value = rowcount + 1;
                cmd.Parameters["Username"].Value = name;
                cmd.Parameters["Messages"].Value = message;
                cmd.Parameters["MessageTime"].Value = DateTime.Now.ToString();


                await cmd.ExecuteNonQueryAsync();

                });
            }
             
            
        }
    }
}

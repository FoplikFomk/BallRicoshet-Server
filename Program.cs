using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();
app.UseWebSockets();

var clients = new ConcurrentDictionary<string, WebSocket>();
var waitingPlayer = "";

app.Map("/ws", async (HttpContext context) =>
{
    if (context.WebSockets.IsWebSocketRequest)
    {
        var webSocket = await context.WebSockets.AcceptWebSocketAsync();
        var playerId = Guid.NewGuid().ToString();
        
        clients.TryAdd(playerId, webSocket);
        Console.WriteLine($"Игрок {playerId} подключился.");

        lock (waitingPlayer)
        {
            if (string.IsNullOrEmpty(waitingPlayer))
            {
                waitingPlayer = playerId;
            }
            else
            {
                var p1 = waitingPlayer;
                var p2 = playerId;
                waitingPlayer = ""; 

                _ = SendMessage(clients[p1], $"MATCH_FOUND:{p2}");
                _ = SendMessage(clients[p2], $"MATCH_FOUND:{p1}");
                Console.WriteLine($"Матч найден: {p1} vs {p2}");
            }
        }

        var buffer = new byte[1024 * 4];
        while (webSocket.State == WebSocketState.Open)
        {
            var result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
            if (result.MessageType == WebSocketMessageType.Close) break;

            var message = Encoding.UTF8.GetString(buffer, 0, result.Count);
            
            if (message.StartsWith("RELAY:"))
            {
                var parts = message.Split(':', 3); // ВАЖНО: ТРОЙКА!
                var targetId = parts[1];
                if (clients.TryGetValue(targetId, out var targetSocket) && targetSocket.State == WebSocketState.Open)
                {
                    var payload = Encoding.UTF8.GetBytes(parts[2]);
                    await targetSocket.SendAsync(new ArraySegment<byte>(payload), WebSocketMessageType.Text, true, CancellationToken.None);
                }
            }
        }

        clients.TryRemove(playerId, out _);
        if (waitingPlayer == playerId) waitingPlayer = "";
    }
});

async Task SendMessage(WebSocket ws, string msg)
{
    var bytes = Encoding.UTF8.GetBytes(msg);
    await ws.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, CancellationToken.None);
}

var port = Environment.GetEnvironmentVariable("PORT") ?? "8080";
app.Run($"http://0.0.0.0:{port}");

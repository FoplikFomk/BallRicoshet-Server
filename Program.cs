using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();
app.UseWebSockets();

var clients = new ConcurrentDictionary<string, WebSocket>();
var rooms = new ConcurrentDictionary<string, string>(); // ID -> RoomID
var playerHps = new ConcurrentDictionary<string, float>();
var waitingPlayer = "";

app.Map("/ws", async (HttpContext context) =>
{
    if (context.WebSockets.IsWebSocketRequest)
    {
        var webSocket = await context.WebSockets.AcceptWebSocketAsync();
        var playerId = Guid.NewGuid().ToString();
        clients.TryAdd(playerId, webSocket);
        playerHps.TryAdd(playerId, 8000);

        string roomId = "";
        string opponentId = "";

        lock (waitingPlayer)
        {
            if (string.IsNullOrEmpty(waitingPlayer))
            {
                waitingPlayer = playerId;
            }
            else
            {
                opponentId = waitingPlayer;
                waitingPlayer = "";
                roomId = Guid.NewGuid().ToString();
                
                rooms[playerId] = roomId;
                rooms[opponentId] = roomId;

                _ = SendMessage(clients[opponentId], $"MATCH_FOUND:{playerId}");
                _ = SendMessage(clients[playerId], $"MATCH_FOUND:{opponentId}");
            }
        }

        var buffer = new byte[1024 * 4];
        while (webSocket.State == WebSocketState.Open)
        {
            var result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
            if (result.MessageType == WebSocketMessageType.Close) break;

            var message = Encoding.UTF8.GetString(buffer, 0, result.Count);
            
            if (message.StartsWith("MOVE:"))
            {
                if (!string.IsNullOrEmpty(opponentId) && clients.TryGetValue(opponentId, out var oppSocket))
                {
                    _ = SendMessage(oppSocket, message.Replace("MOVE:", "POS:"));
                }
            }
            else if (message.StartsWith("HIT:"))
            {
                // Формат: HIT:ID:урон
                var parts = message.Split(':');
                if (parts.Length >= 3 && float.TryParse(parts[2], out float dmg))
                {
                    var targetId = parts[1];
                    if (playerHps.TryGetValue(targetId, out float hp))
                    {
                        hp -= dmg;
                        playerHps[targetId] = hp;
                        if (clients.TryGetValue(targetId, out var targetSocket))
                        {
                            _ = SendMessage(targetSocket, $"DAMAGE:{dmg}");
                            if (hp <= 0) _ = SendMessage(targetSocket, "DEAD");
                        }
                    }
                }
            }
        }
        clients.TryRemove(playerId, out _);
        rooms.TryRemove(playerId, out _);
        playerHps.TryRemove(playerId, out _);
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

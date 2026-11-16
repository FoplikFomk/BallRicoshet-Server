using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();
app.UseWebSockets();

var clients = new ConcurrentDictionary<string, WebSocket>();
var players = new ConcurrentDictionary<string, PlayerState>();
var waitingPlayer = "";

class PlayerState { public float X, Y, Hp = 8000; }

app.Map("/ws", async (HttpContext context) =>
{
    if (context.WebSockets.IsWebSocketRequest)
    {
        var webSocket = await context.WebSockets.AcceptWebSocketAsync();
        var playerId = Guid.NewGuid().ToString();
        var opponentId = ""; // ВАЖНО: объявляем здесь

        clients.TryAdd(playerId, webSocket);
        players.TryAdd(playerId, new PlayerState { X = 250, Y = 540 });

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
                // Отправляем ID друг другу
                _ = SendMessage(clients[opponentId], $"MATCH_FOUND:{playerId}");
                _ = SendMessage(clients[playerId], $"MATCH_FOUND:{opponentId}");
                Console.WriteLine($"Матч: {opponentId} vs {playerId}");
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
                var parts = message.Split(':');
                if (parts.Length >= 3 && float.TryParse(parts[1], out float x) && float.TryParse(parts[2], out float y))
                {
                    players[playerId].X = x;
                    players[playerId].Y = y;
                    
                    // Пересылаем сопернику
                    if (!string.IsNullOrEmpty(opponentId) && clients.TryGetValue(opponentId, out var oppSocket) && oppSocket.State == WebSocketState.Open)
                    {
                        _ = SendMessage(oppSocket, $"POS:{x}:{y}");
                    }
                }
            }
            else if (message.StartsWith("HIT:"))
            {
                var parts = message.Split(':',3);
                if (parts.Length >= 3 && float.TryParse(parts[2], out float damage))
                {
                    var targetId = parts[1];
                    if (players.TryGetValue(targetId, out var target))
                    {
                        target.Hp -= damage;
                        if (clients.TryGetValue(targetId, out var targetSocket) && targetSocket.State == WebSocketState.Open)
                            _ = SendMessage(targetSocket, $"DAMAGE:{damage}");
                    }
                }
            }
        }
        clients.TryRemove(playerId, out _);
        players.TryRemove(playerId, out _);
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

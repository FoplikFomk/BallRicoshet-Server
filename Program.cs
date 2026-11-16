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
        var opponentId = ""; // Важно: объявляем здесь!

        clients.TryAdd(playerId, webSocket);
        Console.WriteLine($"Игрок {playerId} зашел.");

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
                Console.WriteLine($"МАТЧ НАЙДЕН: {opponentId} vs {playerId}");
            }
        }

        var buffer = new byte[1024 * 4];
        while (webSocket.State == WebSocketState.Open)
        {
            var result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
            if (result.MessageType == WebSocketMessageType.Close) break;

            var message = Encoding.UTF8.GetString(buffer, 0, result.Count);
            
            // Если пришла позиция
            if (message.StartsWith("MOVE:"))
            {
                // Пересылаем её сопернику
                if (!string.IsNullOrEmpty(opponentId) && clients.TryGetValue(opponentId, out var oppSocket) && oppSocket.State == WebSocketState.Open)
                {
                    var posMsg = message.Substring(5); // отрезаем "MOVE:"
                    _ = SendMessage(oppSocket, $"POS:{posMsg}");
                }
            }
        }

        clients.TryRemove(playerId, out _);
        if (waitingPlayer == playerId) waitingPlayer = "";
        Console.WriteLine($"Игрок {playerId} вышел.");
    }
});

async Task SendMessage(WebSocket ws, string msg)
{
    var bytes = Encoding.UTF8.GetBytes(msg);
    await ws.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, CancellationToken.None);
}

var port = Environment.GetEnvironmentVariable("PORT") ?? "8080";
app.Run($"http://0.0.0.0:{port}");

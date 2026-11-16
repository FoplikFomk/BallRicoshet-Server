using System.Collections.Concurrent;
using System.Net;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

// Список игроков в очереди (IP адрес)
var waitingPlayer = "";

// Эндпоинт для поиска игры
app.MapGet("/find_match", (HttpContext context) =>
{
    var clientIp = context.Connection.RemoteIpAddress?.ToString() ?? "";
    
    lock (waitingPlayer)
    {
        if (string.IsNullOrEmpty(waitingPlayer))
        {
            // Никого нет, этот игрок становится первым в очереди
            waitingPlayer = clientIp;
            return Results.Text("WAITING");
        }
        else
        {
            // Нашли второго игрока!
            var opponentIp = waitingPlayer;
            waitingPlayer = ""; // Очищаем очередь
            
            // Отдаем IP соперника
            return Results.Text($"MATCH_FOUND:{opponentIp}");
        }
    }
});

app.Run();

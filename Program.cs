using System.Collections.Concurrent;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

// Очередь игроков
var waitingPlayerIp = "";

app.MapGet("/find_match", (HttpContext context) =>
{
    var ip = context.Connection.RemoteIpAddress?.ToString() ?? "0.0.0.0";
    
    lock (waitingPlayerIp)
    {
        if (string.IsNullOrEmpty(waitingPlayerIp))
        {
            waitingPlayerIp = ip;
            return Results.Text("WAITING");
        }
        else
        {
            var opponent = waitingPlayerIp;
            waitingPlayerIp = ""; // Очищаем
            return Results.Text($"MATCH_FOUND:{opponent}");
        }
    }
});

// Запуск на порту, который даст Render
var port = Environment.GetEnvironmentVariable("PORT") ?? "8080";
app.Run($"http://0.0.0.0:{port}");

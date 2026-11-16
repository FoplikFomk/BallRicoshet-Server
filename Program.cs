using System.Globalization;
using System.Net.WebSockets;
using System.Text;
using System.Text.RegularExpressions;

// ===================== BallRicoshet relay + authoritative HP server =====================
// Протокол (текст, разделитель ':'):
//  Клиент -> сервер: READY:{fighterId}:{maxHp} | S:{payload} | HIT:{damage} | HEAL:{amount}
//  Сервер -> клиент: HELLO:{id} | MATCH_FOUND:{oppId}:{slot} | START:{oppFighterId} | REQUEUE
//                    S:{payload} | HP:{myHp}:{oppHp} | DEAD | WIN | OPP_LEFT
// Комната = ровно 2 игрока. Первый в очереди получает slot 0, второй slot 1.
// Третий игрок ждёт в очереди и попадает в НОВУЮ комнату с четвёртым.

var builder = WebApplication.CreateBuilder(args);
var port = Environment.GetEnvironmentVariable("PORT") ?? "10000";
builder.WebHost.UseUrls($"http://0.0.0.0:{port}");

var app = builder.Build();
app.UseWebSockets(new WebSocketOptions { KeepAliveInterval = TimeSpan.FromSeconds(20) });

app.MapGet("/", () => "BallRicoshet server is running");
app.MapGet("/health", () => Results.Ok(new { ok = true, waiting = Lobby.WaitingCount }));

app.Map("/ws", async (HttpContext ctx) =>
{
    if (!ctx.WebSockets.IsWebSocketRequest)
    {
        ctx.Response.StatusCode = 400;
        return;
    }
    using var ws = await ctx.WebSockets.AcceptWebSocketAsync();
    await GameServer.HandleAsync(ws);
});

app.Run();

// ===================== Типы =====================

sealed class Player
{
    public readonly string Id = Guid.NewGuid().ToString("N")[..8];
    public readonly WebSocket Socket;
    public volatile Room? Room;
    public bool Ready;
    public string FighterId = "pruzek";
    public float MaxHp = 8000f;
    public float Hp = 8000f;
    public DateTime LastHeal = DateTime.UtcNow;
    private readonly SemaphoreSlim _sendLock = new(1, 1);

    public Player(WebSocket socket) => Socket = socket;

    public async Task SendAsync(string msg)
    {
        if (Socket.State != WebSocketState.Open) return;
        await _sendLock.WaitAsync();
        try
        {
            var bytes = Encoding.UTF8.GetBytes(msg);
            await Socket.SendAsync(new ArraySegment<byte>(bytes), WebSocketMessageType.Text, true, CancellationToken.None);
        }
        catch { /* сокет умер — обработает Disconnect */ }
        finally { _sendLock.Release(); }
    }
}

sealed class Room
{
    public readonly object Gate = new();
    public readonly Player A, B;
    public bool Started, Over;

    public Room(Player a, Player b) { A = a; B = b; }
    public Player Other(Player p) => p == A ? B : A;
}

// ===================== Очередь и комнаты =====================

static class Lobby
{
    private static readonly object Gate = new();
    private static Player? _waiting;

    public static int WaitingCount { get { lock (Gate) return _waiting != null ? 1 : 0; } }

    public static async Task EnqueueAsync(Player p)
    {
        if (p.Socket.State != WebSocketState.Open) return;

        Player? partner = null;
        lock (Gate)
        {
            // Ждущий игрок должен быть жив, иначе (мёртвый сокет) просто заменяем его
            if (_waiting != null && _waiting != p && _waiting.Socket.State == WebSocketState.Open)
            {
                partner = _waiting;
                _waiting = null;
            }
            else
            {
                _waiting = p;
            }
        }
        if (partner == null) return;

        // partner пришёл первым -> slot 0
        var room = new Room(partner, p);
        partner.Room = room;
        p.Room = room;
        partner.Ready = false;
        p.Ready = false;

        await partner.SendAsync($"MATCH_FOUND:{p.Id}:0");
        await p.SendAsync($"MATCH_FOUND:{partner.Id}:1");
        _ = StartTimeoutAsync(room);
    }

    // Если кто-то не прислал READY за 15 сек — отключаем его, второго вернём в очередь
    private static async Task StartTimeoutAsync(Room room)
    {
        await Task.Delay(TimeSpan.FromSeconds(15));
        bool pending;
        lock (room.Gate) pending = !room.Started && !room.Over;
        if (!pending) return;

        foreach (var pl in new[] { room.A, room.B })
            if (!pl.Ready)
                try { pl.Socket.Abort(); } catch { }
    }

    public static async Task DisconnectAsync(Player p)
    {
        lock (Gate)
        {
            if (_waiting == p) _waiting = null;
        }

        var room = p.Room;
        if (room == null) return;
        p.Room = null;

        Player other;
        bool wasStarted, wasOver;
        lock (room.Gate)
        {
            other = room.Other(p);
            wasOver = room.Over;
            wasStarted = room.Started;
            room.Over = true;
        }
        if (wasOver) return;

        other.Room = null;
        if (wasStarted)
        {
            await other.SendAsync("OPP_LEFT");            // соперник вышел посреди боя -> победа
        }
        else
        {
            other.Ready = false;                          // матч ещё не начался -> ищем нового соперника
            await other.SendAsync("REQUEUE");
            await EnqueueAsync(other);
        }
    }
}

// ===================== Обработка клиента =====================

static class GameServer
{
    private const float MaxHitDamage = 2500f;   // больше одного попадания быть не может
    private const float HealPerSecCap = 600f;   // регенерация Прузека 350/сек + запас

    public static async Task HandleAsync(WebSocket ws)
    {
        var p = new Player(ws);
        var buf = new byte[4096];
        var ms = new MemoryStream();

        try
        {
            await p.SendAsync($"HELLO:{p.Id}");
            await Lobby.EnqueueAsync(p);

            while (ws.State == WebSocketState.Open)
            {
                var msg = await ReceiveTextAsync(ws, buf, ms);
                if (msg == null) break;
                await DispatchAsync(p, msg);
            }
        }
        catch { /* обрыв соединения */ }
        finally
        {
            await Lobby.DisconnectAsync(p);
            try { ws.Abort(); } catch { }
        }
    }

    private static async Task<string?> ReceiveTextAsync(WebSocket ws, byte[] buf, MemoryStream ms)
    {
        ms.SetLength(0);
        WebSocketReceiveResult r;
        do
        {
            r = await ws.ReceiveAsync(new ArraySegment<byte>(buf), CancellationToken.None);
            if (r.MessageType == WebSocketMessageType.Close) return null;
            ms.Write(buf, 0, r.Count);
            if (ms.Length > 16 * 1024) return null;
        } while (!r.EndOfMessage);
        return Encoding.UTF8.GetString(ms.GetBuffer(), 0, (int)ms.Length);
    }

    private static async Task DispatchAsync(Player p, string msg)
    {
        int i = msg.IndexOf(':');
        string cmd = i < 0 ? msg : msg[..i];
        string arg = i < 0 ? "" : msg[(i + 1)..];

        switch (cmd)
        {
            case "READY": await OnReadyAsync(p, arg); break;
            case "S": await OnStateAsync(p, arg); break;
            case "HIT": await OnHitAsync(p, arg); break;
            case "HEAL": await OnHealAsync(p, arg); break;
            case "LEAVE": p.Socket.Abort(); break;
        }
    }

    private static async Task OnReadyAsync(Player p, string arg)
    {
        var room = p.Room;
        if (room == null) return;

        var parts = arg.Split(':');
        string fid = parts.Length > 0 && Regex.IsMatch(parts[0], "^[a-z0-9_]{1,24}$") ? parts[0] : "pruzek";
        float maxHp = 8000f;
        if (parts.Length > 1 && float.TryParse(parts[1], NumberStyles.Float, CultureInfo.InvariantCulture, out var m))
            maxHp = Math.Clamp(m, 1000f, 30000f);

        bool start = false;
        lock (room.Gate)
        {
            if (room.Over || room.Started) return;
            p.FighterId = fid;
            p.MaxHp = maxHp;
            p.Hp = maxHp;
            p.Ready = true;
            if (room.A.Ready && room.B.Ready)
            {
                room.Started = true;
                start = true;
            }
        }

        if (start)
        {
            await room.A.SendAsync($"START:{room.B.FighterId}");
            await room.B.SendAsync($"START:{room.A.FighterId}");
        }
    }

    // Положение, прицел, атаки и способности — просто пересылаем сопернику в той же комнате
    private static async Task OnStateAsync(Player p, string payload)
    {
        var room = p.Room;
        if (room == null) return;
        Player other;
        lock (room.Gate)
        {
            if (!room.Started || room.Over) return;
            other = room.Other(p);
        }
        await other.SendAsync("S:" + payload);
    }

    // Стрелок сообщает о попадании; ТОЛЬКО сервер уменьшает HP соперника
    private static async Task OnHitAsync(Player shooter, string arg)
    {
        var room = shooter.Room;
        if (room == null) return;
        if (!float.TryParse(arg, NumberStyles.Float, CultureInfo.InvariantCulture, out var dmg)) return;
        dmg = Math.Clamp(dmg, 0f, MaxHitDamage);
        if (dmg <= 0f) return;

        Player victim;
        bool died = false;
        lock (room.Gate)
        {
            if (!room.Started || room.Over) return;
            victim = room.Other(shooter);
            if (victim.Hp <= 0f) return;
            victim.Hp = Math.Max(0f, victim.Hp - dmg);
            if (victim.Hp <= 0f)
            {
                room.Over = true;
                died = true;
            }
        }

        await SendHpAsync(room);
        if (died)
        {
            await victim.SendAsync("DEAD");
            await shooter.SendAsync("WIN");
        }
    }

    private static async Task OnHealAsync(Player p, string arg)
    {
        var room = p.Room;
        if (room == null) return;
        if (!float.TryParse(arg, NumberStyles.Float, CultureInfo.InvariantCulture, out var amount)) return;

        bool changed = false;
        var now = DateTime.UtcNow;
        lock (room.Gate)
        {
            double sec = Math.Min((now - p.LastHeal).TotalSeconds, 1.0);
            p.LastHeal = now;
            if (!room.Started || room.Over || p.Hp <= 0f) return;

            float allowed = (float)(sec * HealPerSecCap) + 60f;   // защита от накрутки лечения
            amount = Math.Clamp(amount, 0f, allowed);
            float before = p.Hp;
            p.Hp = Math.Min(p.MaxHp, p.Hp + amount);
            changed = p.Hp > before + 0.01f;
        }
        if (changed) await SendHpAsync(room);
    }

    private static async Task SendHpAsync(Room room)
    {
        int a, b;
        lock (room.Gate)
        {
            a = (int)MathF.Ceiling(room.A.Hp);
            b = (int)MathF.Ceiling(room.B.Hp);
        }
        await room.A.SendAsync($"HP:{a}:{b}");
        await room.B.SendAsync($"HP:{b}:{a}");
    }
}

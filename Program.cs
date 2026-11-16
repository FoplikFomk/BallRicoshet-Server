using LiteNetLib;
using LiteNetLib.Utils;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;

namespace BallRicoshet.Server;

public class Server : INetEventListener
{
    private NetManager? _netManager;
    private List<NetPeer> _players = new();

    public void Start()
    {
        _netManager = new NetManager(this);
        _netManager.Start(9050); // Порт сервера
        Console.WriteLine("Сервер запущен на порту 9050!");
    }

    public void Update() => _netManager?.PollEvents();

    public void OnConnectionRequest(ConnectionRequest request)
    {
        request.AcceptIfKey("BallRicoshetKey");
    }

    public void OnPeerConnected(NetPeer peer)
    {
        Console.WriteLine($"Игрок подключился: {peer.Address}");
        _players.Add(peer);

        if (_players.Count == 2)
        {
            var p1 = _players[0];
            var p2 = _players[1];

            var writer = new NetDataWriter();
            writer.Put("MATCH_FOUND");
            writer.Put(p2.Address.ToString());
            p1.Send(writer, DeliveryMethod.ReliableOrdered);

            writer = new NetDataWriter();
            writer.Put("MATCH_FOUND");
            writer.Put(p1.Address.ToString());
            p2.Send(writer, DeliveryMethod.ReliableOrdered);

            _players.Clear();
            Console.WriteLine("Пара найдена!");
        }
    }

    public void OnPeerDisconnected(NetPeer peer, DisconnectInfo disconnectInfo) => _players.Remove(peer);
    public void OnNetworkError(IPEndPoint endPoint, SocketError socketError) { }
    public void OnNetworkReceive(NetPeer peer, NetPacketReader reader, byte channelNumber, DeliveryMethod deliveryMethod) { }
    public void OnNetworkReceiveUnconnected(IPEndPoint remoteEndPoint, NetPacketReader reader, UnconnectedMessageType messageType) { }
    public void OnNetworkLatencyUpdate(NetPeer peer, int latency) { }
}

public class Program
{
    public static void Main()
    {
        var server = new Server();
        server.Start();
        while (true) { server.Update(); }
    }
}

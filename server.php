<?php
use Workerman\Worker;
use PHPSocketIO\SocketIO;
use Channel\Server as ChannelServer;
use Channel\Client as ChannelClient;

require_once __DIR__ . '/vendor/autoload.php';

// Force Workerman to use the project root for logs and process management
Worker::$logFile = __DIR__ . '/workerman.log';
Worker::$pidFile = __DIR__ . '/workerman.pid';

// 1. Initialize internal Channel Server for inter-process communication (IPC)
$channel_server = new ChannelServer('127.0.0.1', 2206);

// 2. Initialize the Socket.IO server listening internally on port 8080
$io = new SocketIO(8181);

$io->on('workerStart', function() use ($io) {
    // Connect the Socket.IO worker process to the internal Channel cluster
    ChannelClient::connect('127.0.0.1', 2206);
    
    // Listen for incoming system-wide API broadcast events from the HTTP worker
    ChannelClient::on('api_broadcast', function($data) use ($io) {
        $roomType = $data['room_type'] ?? 'public';
        $roomId   = $data['room_id'] ?? '';
        $message  = $data['message'] ?? '';
        $roomName = "{$roomType}:{$roomId}";
        
        // Broadcast the event into the specific Socket.io room pool
        $io->to($roomName)->emit('message_received', [
            'room'      => $roomName,
            'sender'    => 'SYSTEM_API',
            'message'   => $message,
            'timestamp' => time()
        ]);
    });
});

$io->on('connection', function ($socket) use ($io) {
    echo "New connection established. Socket ID: {$socket->id}\n";

    /**
     * Action: Join a Public or Private Room
     */
    $socket->on('join_room', function ($data) use ($socket) {
        $roomType = $data['room_type'] ?? 'public';
        $roomId   = $data['room_id'] ?? '';
        $roomName = "{$roomType}:{$roomId}";

        if (empty($roomId)) {
            $socket->emit('error_msg', 'Room ID cannot be empty.');
            return;
        }

        if ($roomType === 'private') {
            $token = $data['token'] ?? '';
            if ($token !== 'php-pubsub-token-2o26') {
                $socket->emit('error_msg', 'Access denied. Invalid token.');
                return;
            }
        }

        $socket->join($roomName);
        $socket->emit('status', "Successfully joined room: {$roomName}");
        $socket->to($roomName)->emit('notification', "User {$socket->id} entered the room.");
    });

    /**
     * Action: Publish a message to a Room (From WebSocket Client)
     */
    $socket->on('publish_message', function ($data) use ($socket, $io) {
        $roomType = $data['room_type'] ?? 'public';
        $roomId   = $data['room_id'] ?? '';
        $roomName = "{$roomType}:{$roomId}";
        $message  = $data['message'] ?? '';

        if ($roomType === 'private' && !isset($socket->rooms[$roomName])) {
            $socket->emit('error_msg', 'You must join this room before broadcasting messages.');
            return;
        }

        $io->to($roomName)->emit('message_received', [
            'room'      => $roomName,
            'sender'    => $socket->id,
            'message'   => $message,
            'timestamp' => time()
        ]);
    });

    /**
     * Action: Leave a Room
     */
    $socket->on('leave_room', function ($data) use ($socket) {
        $roomName = ($data['room_type'] ?? 'public') . ":" . ($data['room_id'] ?? '');
        $socket->leave($roomName);
        $socket->emit('status', "Left room: {$roomName}");
    });
});


// 3. Initialize standalone HTTP REST API Server on port 8081
$http_api = new Worker("http://0.0.0.0:8081");

$http_api->onWorkerStart = function() {
    // Connect the HTTP process to the internal Channel cluster
    ChannelClient::connect('127.0.0.1', 2206);
};

$http_api->onMessage = function ($connection, $request) {
    $path = $request->path();
    $body = json_decode($request->rawBody(), true) ?? [];
    
    // Routing Match for Broadcast Endpoints
    if ($path === '/api/broadcast/public' || $path === '/api/broadcast/private') {
        $roomType = ($path === '/api/broadcast/private') ? 'private' : 'public';
        $roomId   = $body['room_id'] ?? null;
        $message  = $body['message'] ?? null;
        
        if (!$roomId || !$message) {
            $connection->send(new Workerman\Protocols\Http\Response(400, [
                'Content-Type' => 'application/json'
            ], json_encode(['error' => 'Missing room_id or message fields.'])));
            return;
        }
        
        // Ship data across internal memory bus straight to the Socket.IO process loop
        ChannelClient::publish('api_broadcast', [
            'room_type' => $roomType,
            'room_id'   => $roomId,
            'message'   => $message
        ]);
        
        $connection->send(new Workerman\Protocols\Http\Response(200, [
            'Content-Type' => 'application/json'
        ], json_encode([
            'status'  => 'success',
            'message' => ucfirst($roomType) . " message sent to broadcast loop."
        ])));
        return;
    }
    
    // Default 404 Route
    $connection->send(new Workerman\Protocols\Http\Response(404, [
        'Content-Type' => 'application/json'
    ], json_encode(['error' => 'Endpoint not found. Use /api/broadcast/public or /api/broadcast/private'])));
};

// Run all loops (IPC Server, Socket.io Server, and HTTP Server)
Worker::runAll();
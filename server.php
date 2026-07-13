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
    echo "[Socket.IO Worker] Connected to Channel cluster.\n";
    
    // Listen for incoming system-wide API broadcast events from the HTTP worker
    ChannelClient::on('api_broadcast', function($data) use ($io) {
        $roomType = $data['room_type'] ?? 'public';
        $roomId   = $data['room_id'] ?? '';
        $message  = $data['message'] ?? '';
        $roomName = "{$roomType}:{$roomId}";

        echo "[Socket.IO Engine] Natively caught 'api_broadcast' from internal Channel.\n";
        echo "                    Target Room Name: '{$roomName}'\n";
        echo "                    Message Payload: '{$message}'\n";

        // Count active clients in this room via the Socket.io adapter layer
        $roomClientsCount = 0;
        if (isset($io->sockets->adapter->rooms[$roomName])) {
            $roomClientsCount = count($io->sockets->adapter->rooms[$roomName]);
        }
        
        // Broadcast the event into the specific Socket.io room pool
        $io->to($roomName)->emit('message_received', [
            'room'      => $roomName,
            'sender'    => 'SYSTEM_API',
            'message'   => $message,
            'timestamp' => time()
        ]);

        echo "[Socket.IO Engine] Active subscribers found in '{$roomName}': {$roomClientsCount}\n";
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

        echo "[Client Event] Socket '{$socket->id}' requested to join room: '{$roomName}'\n";

        if (empty($roomId)) {
            echo "[Client Event ERROR] Socket '{$socket->id}' provided empty room_id.\n";
            $socket->emit('error_msg', 'Room ID cannot be empty.');
            return;
        }

        if ($roomType === 'private') {
            $token = $data['token'] ?? '';
            if ($token !== 'php-pubsub-token-2o26') {
                echo "[Client Event ERROR] Socket '{$socket->id}' failed token authentication for room '{$roomName}'.\n";
                $socket->emit('error_msg', 'Access denied. Invalid token.');
                return;
            }
        }

        $socket->join($roomName);
        echo "[Client Event SUCCESS] Socket '{$socket->id}' added to room: '{$roomName}'\n";
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
        echo "[Client Event] Socket '{$socket->id}' voluntarily left room: '{$roomName}'\n";
        $socket->leave($roomName);
        $socket->emit('status', "Left room: {$roomName}");
    });

    $socket->on('disconnect', function() use ($socket) {
        echo "[Socket.IO Worker] Connection closed for Socket ID: {$socket->id}\n";
    });
});


// 3. Initialize standalone HTTP REST API Server on port 8081
$http_api = new Worker("http://0.0.0.0:8081");

$http_api->onWorkerStart = function() {
    // Connect the HTTP process to the internal Channel cluster
    ChannelClient::connect('127.0.0.1', 2206);
    echo "[HTTP API Worker] Initialized and connected to Channel cluster.\n";
};

$http_api->onMessage = function ($connection, $request) {
    $path = $request->path();
    $body = json_decode($request->rawBody(), true) ?? [];

    echo "[HTTP API] Received request on route: {$path}\n";
    echo "           Raw Request Content: '{$rawBody}'\n";
    
    // Routing Match for Broadcast Endpoints
    if ($path === '/pubsub/api/broadcast/public' || $path === '/pubsub/api/broadcast/private') {
        $roomType = ($path === '/pubsub/api/broadcast/private') ? 'private' : 'public';
        $roomId   = $body['room_id'] ?? null;
        $message  = $body['message'] ?? null;
        
        if (!$roomId || !$message) {
            echo "[HTTP API ERROR] Missing payload properties. room_id: '" . ($roomId??"NULL") . "', message: '" . ($message??"NULL") . "'\n";
            $connection->send(new Workerman\Protocols\Http\Response(400, [
                'Content-Type' => 'application/json'
            ], json_encode(['error' => 'Missing room_id or message fields.'])));
            return;
        }

        echo "[HTTP API SUCCESS] Payload validated. Publishing to internal Channel bus...\n";
        
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
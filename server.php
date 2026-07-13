<?php
use Workerman\Worker;
use PHPSocketIO\SocketIO;

require_once __DIR__ . '/vendor/autoload.php';

Worker::$logFile = __DIR__ . '/workerman.log';
Worker::$pidFile = __DIR__ . '/workerman.pid';

// Instantiate the Socket.IO server listening internally on port 8080
$io = new SocketIO(8181);

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

        // Authorization Gateway for Private Channels
        if ($roomType === 'private') {
            $token = $data['token'] ?? '';
            if ($token !== 'php-pubsub-token-2o26') { // Replace with DB lookup or JWT validation
                $socket->emit('error_msg', 'Access denied. Invalid token.');
                return;
            }
        }

        // Native Socket.io Room join method (automatically populates $socket->rooms[$roomName])
        $socket->join($roomName);

        // Acknowledge the joining client
        $socket->emit('status', "Successfully joined room: {$roomName}");
        
        // Broadcast a notification to everyone else already in that room
        $socket->to($roomName)->emit('notification', "User {$socket->id} entered the room.");
    });

    /**
     * Action: Publish a message to a Room
     */
    $socket->on('publish_message', function ($data) use ($socket, $io) {
        $roomType = $data['room_type'] ?? 'public';
        $roomId   = $data['room_id'] ?? '';
        $roomName = "{$roomType}:{$roomId}";
        $message  = $data['message'] ?? '';

        // Verification guard: using the native framework array to check room subscription status
        if ($roomType === 'private' && !isset($socket->rooms[$roomName])) {
            $socket->emit('error_msg', 'You must join this room before broadcasting messages.');
            return;
        }

        // Broadcast payload to ALL users inside the target room (including sender)
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
        
        // Native Socket.io leave method (automatically purges from $socket->rooms)
        $socket->leave($roomName);
        
        $socket->emit('status', "Left room: {$roomName}");
    });
});

// Run the core engine loops
Worker::runAll();
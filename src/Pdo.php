<?php

namespace AppKit\Database\Pdo;

use AppKit\StartStop\StartStopInterface;
use AppKit\Health\HealthIndicatorInterface;
use AppKit\Health\HealthCheckResult;
use AppKit\Database\DatabaseInterface;
use AppKit\Async\Task;
use AppKit\Async\CanceledException;
use function AppKit\Async\delay;

use React\ChildProcess\Process;
use React\Promise\Deferred;
use Throwable;
use function React\Async\async;
use function React\Async\await;

class Pdo implements StartStopInterface, HealthIndicatorInterface, DatabaseInterface {
    private $dsn;
    private $user;
    private $password;

    private $log;
    private $isStopping = false;
    private $process;
    private $exitDeferred;
    private $buffer;
    private $connectTask;
    private $isConnected = false;
    private $requests = [];
    private $transaction;
    private $statements = [];
    private $destroyedStatements = [];

    /****************************************
     * CONSTRUCTOR
     ****************************************/

    function __construct(
        $log,
        $dsn,
        $user = null,
        $password = null,
        $name = null
    ) {
        $this -> dsn = $dsn;
        $this -> user = $user;
        $this -> password = $password;

        $this -> log = $log -> withModule($this, $name);
    }

    /****************************************
     * GENERAL INTERFACE METHODS
     ****************************************/

    public function start() {
        $this -> spawnWorker();
    }

    public function stop() {
        $this -> isStopping = true;

        if($this -> connectTask -> getStatus() == Task::RUNNING) {
            $this -> log -> debug('Connect task running during stop, canceling...');
            $this -> connectTask -> cancel() -> join();
        }

        if($this -> isConnected) {
            $requestCount = count($this -> requests);
            if($requestCount > 0) {
                $this -> log -> warning("Waiting for $requestCount pending requests before stop...");
                try {
                    await($this -> requests[$requestCount - 1]);
                } catch(Throwable $e) {}
            }
        }

        $this -> stopWorker();
    }

    public function checkHealth() {
        return new HealthCheckResult([
            'Worker running' => $this -> process && $this -> process -> isRunning(),
            'Connected to database' => $this -> isConnected
        ]);
    }

    /****************************************
     * DATABASE INTERFACE METHODS
     ****************************************/

    public function prepare($query) {
        $this -> checkNoTransaction();

        return $this -> prepareInternal($query);
    }

    public function query($query, $params = []) {
        $this -> checkNoTransaction();

        return $this -> queryInternal($query, $params);
    }

    public function beginTransaction() {
        $this -> checkNoTransaction();

        $this -> request('beginTransaction');

        $id = bin2hex(random_bytes(8));
        $this -> transaction = $id;
        return new PdoTransaction($this, $id);
    }

    /****************************************
     * TRANSACTION INTERNALS
     ****************************************/

    public function _tx_destroy($id) {
        if($id !== $this -> transaction)
            return;

        $this -> log -> warning(
            "Transaction destroyed without commit or rollback, performing automatic rollback"
        );

        try {
            $this -> rollBackInternal();
            $this -> log -> info("Rolled back transaction");
        } catch(Throwable $e) {
            $this -> log -> error("Failed to rollback transaction", $e);
        }
    }

    public function _tx_prepare($id, $query) {
        $this -> checkTransaction($id);

        return $this -> prepareInternal($query, $id);
    }

    public function _tx_query($id, $query, $params) {
        $this -> checkTransaction($id);

        return $this -> queryInternal($query, $params, $id);
    }

    public function _tx_commit($id) {
        $this -> checkTransaction($id);

        $this -> request('commit');
        $this -> transaction = null;
    }

    public function _tx_rollBack($id) {
        $this -> checkTransaction($id);

        $this -> rollBackInternal();
    }

    private function checkNoTransaction() {
        if($this -> transaction)
            throw new PdoException('Operation not allowed during a transaction');
    }

    private function checkTransaction($id) {
        if($id !== $this -> transaction)
            throw new PdoException('Transaction detached from the current connection');
    }

    private function rollBackInternal() {
        $this -> request('rollBack');
        $this -> transaction = null;
    }

    /****************************************
     * STATEMENT INTERNALS
     ****************************************/

    public function _stmt_destroy($id) {
        if(! isset($this -> statements[$id]))
            return;

        unset($this -> statements[$id]);
        $this -> destroyedStatements[] = $id;
    }

    public function _stmt_execute($id, $params) {
        $this -> checkStatement($id);
        $this -> checkStatementTransaction($id);

        return $this -> request(
            ['execute', 'fetch'],
            ['id' => $id, 'params' => $params]
        );
    }

    public function _stmt_fetch($id, $mode) {
        $this -> checkStatement($id);

        return $this -> request(
            'fetch',
            ['id' => $id, 'mode' => $mode]
        );
    }

    private function checkStatement($id) {
        if(! array_key_exists($id, $this -> statements))
            throw new PdoException('Statement detached from the current connection');
    }

    private function checkStatementTransaction($id) {
        if($this -> statements[$id] !== $this -> transaction)
            throw new PdoException(
                'Statement transaction context does not match the current one'
            );
    }

    private function prepareInternal($query, $txid = null) {
        return $this -> createStatement(
            'prepare',
            ['query' => $query],
            $txid
        );
    }

    private function queryInternal($query, $params, $txid = null) {
        return $this -> createStatement(
            ['prepare', 'execute', 'fetch'],
            ['query' => $query, 'params' => $params],
            $txid
        );
    }

    private function createStatement($cmd, $params, $txid) {
        $id = bin2hex(random_bytes(8));
        $resp = $this -> request($cmd, ['id' => $id] + $params);
        $this -> statements[$id] = $txid;
        return new PdoStatement($this, $id, $resp);
    }

    /****************************************
     * SUBPROCESS INTERNALS
     ****************************************/

    private function spawnWorker() {
        $this -> buffer = '';

        try {
            $this -> process = new Process(
                'trap \'\' INT; php '.__DIR__.'/../bin/pdo_worker.php'
            );
            $this -> process -> start();
            $this -> process -> stdout -> on('data', function($chunk) {
                return $this -> onWorkerData($chunk);
            });
            $this -> process -> on('exit', async(function($exitCode, $termSignal) {
                return $this -> onWorkerExit($exitCode, $termSignal);
            }));

            $this -> log -> info("Spawned worker process");
        } catch(Throwable $e) {
            $error = "Failed to spawn worker process";
            $this -> log -> error($error, $e);
            throw new PdoException($error, previous: $e);
        }

        try {
            $this -> connect();
        } catch(CanceledException $e) {
            $this -> stopWorker();
        }
    }

    private function stopWorker() {
        $this -> exitDeferred = new Deferred();

        try {
            $this -> requestInternal('exit');
        } catch(Throwable $e) {
            $error = 'Failed to send exit command to the worker';
            $this -> log -> error($error, $e);
            throw new PdoException($error, previous: $e);
        }

        await($this -> exitDeferred -> promise());
        $this -> log -> info('Stopped worker process');
    }

    private function onWorkerData($chunk) {
        $this -> buffer .= $chunk;

        while(($bufferLen = strlen($this -> buffer)) >= 4) {
            $len = unpack('N', substr($this -> buffer, 0, 4))[1];
            if($bufferLen < $len + 4)
                break;

            $frame = substr($this -> buffer, 4, $len);
            $this -> buffer = substr($this -> buffer, 4 + $len);
            $this -> onResponse($frame);
        }
    }

    private function onWorkerExit($exitCode, $termSignal) {
        $this -> onConnectionClose(false);

        foreach($this -> requests as $request) {
            $request -> reject(new PdoException('Worker process failed while handling the request'));
        }
        $this -> requests = [];

        if($termSignal !== null) {
            $this -> log -> error("Worker process terminated with signal $termSignal, respawning...");
        } else if($exitCode == 0) {
            $this -> log -> debug("Worker process exited cleanly");
            $this -> exitDeferred -> resolve(null);
            return;
        } else {
            $this -> log -> error("Worker process failed with code $exitCode, respawning...");
        }

        $this -> spawnWorker();
    }

    /****************************************
     * CONNECTION INTERNALS
     ****************************************/

    private function connect() {
        $this -> log -> debug('Starting connect task...');

        $this -> connectTask = new Task(function() {
            return $this -> connectRoutine();
        });

        try {
            $this -> connectTask -> run() -> await();
            $this -> log -> debug('Connect task completed');
        } catch(CanceledException $e) {
            $this -> log -> info('Connect task canceled');
        }
    }

    private function connectRoutine() {
        $connectDelay = null;

        while(true) {
            $this -> log -> debug('Trying to connect to the database...');

            try {
                $this -> requestInternal('connect', [
                    'dsn' => $this -> dsn,
                    'user' => $this -> user,
                    'password' => $this -> password
                ]);
                $this -> log -> info('Connected to the database');
                break;
            } catch(Throwable $e) {
                if(! $connectDelay)
                    $connectDelay = 1;
                else if($connectDelay == 1)
                    $connectDelay = 5;
                else if($connectDelay == 5)
                    $connectDelay = 10;

                $this -> log -> error(
                    "Failed to connect to the database, retrying in $connectDelay seconds",
                    $e
                );
                delay($connectDelay);
            }
        }

        $this -> isConnected = true;
    }

    private function onConnectionClose($reconnect = true) {
        if(! $this -> isConnected)
            return;
        $this -> isConnected = false;

        if($this -> transaction) {
            $this -> log -> warning("Connection closed during the transaction");
            $this -> transaction = null;
        }

        $stmtCount = count($this -> statements);
        if($stmtCount > 0) {
            $this -> log -> debug("Connection closed with $stmtCount statements still in use");
            $this -> statements = [];
        }

        $this -> destroyedStatements = [];

        if($this -> isStopping || !$reconnect)
            return;

        $this -> log -> warning('Database connection lost, reconnecting...');
        $this -> connect();
    }

    /****************************************
     * REQUEST / RESPONSE INTERNALS
     ****************************************/

    private function request($cmd, $args = []) {
        if($this -> isStopping)
            throw new PdoException('Database client is shutting down');
        if(! $this -> isConnected)
            throw new PdoException('Not connected');

        return $this -> requestInternal($cmd, $args);
    }

    private function requestInternal($cmd, $args = []) {
        if(! is_array($cmd))
            $cmd = [$cmd];

        $data = igbinary_serialize(
            $x = [
                'cmd' => $cmd,
                'destroy' => $this -> destroyedStatements
            ] + $args
        );
        if($data === false)
            throw new PdoException('Failed to serialize worker request');

        if(
            ! $this -> process -> stdin -> write(pack('N', strlen($data))) ||
            ! $this -> process -> stdin -> write($data)
        ) {
            $error = 'Failed to send request to the worker';
            $this -> log -> error($error);
            throw new PdoException($error);
        }

        $this -> destroyedStatements = [];

        $reqDeferred = new Deferred();
        $this -> requests[] = $reqDeferred;
        return await($reqDeferred -> promise());
    }

    private function onResponse($data) {
        $request = array_shift($this -> requests);

        $resp = igbinary_unserialize($data);
        if($resp === false) {
            $error = 'Received corrupted response from the worker';
            $this -> log -> error($error);
            $request -> reject(new PdoException($error));
            return;
        }

        if(! $resp['success']) {
            if($resp['closed'])
                async(function() { $this -> onConnectionClose(); })();

            if($resp['exception'] == 'PDOException') {
                $request -> reject(
                    new PdoBackendException($resp['message'], $resp['code'])
                );
            } else {
                $request -> reject(
                    new PdoException('Worker error: ' . $resp['message'])
                );
            }

            return;
        }

        $request -> resolve($resp);
    }
}

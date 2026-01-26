#!/usr/bin/env php
<?php

stream_set_blocking(STDIN, true);

$pdo = null;
$statements = [];
$exit = false;

while(true) {
    try {
        $buffer = fread(STDIN, 4);
        if($buffer === false)
            die(1); // Main process failed
        $len = unpack('N', $buffer)[1];

        $buffer = fread(STDIN, $len);
        if($buffer === false)
            die(1); // Main process failed
        $req = igbinary_unserialize($buffer);

        $resp = [
            'success' => true
        ];

        foreach($req['cmd'] as $cmd) switch($cmd) {
            case 'prepare': // [id, query] => []
                $statements[ $req['id'] ] = $pdo -> prepare($req['query']);
                break;

            case 'execute': // [id, params] => [rowCount]
                $id = $req['id'];
                $statements[$id] -> execute($req['params']);
                $resp['rowCount'] = $statements[$id] -> rowCount();
                break;

            case 'fetch': // [id, ?mode] => [rows, end]
                $mode = ($req['mode'] ?? 0) ? PDO::FETCH_NUM    // 1
                                            : PDO::FETCH_ASSOC; // 0, not set

                $resp['rows'] = [];
                $resp['end'] = false;

                $size = 0;
                do {
                    $row = $statements[ $req['id'] ] -> fetch($mode);
                    if($row === false) {
                        $resp['end'] = true;
                        break;
                    }
                    $resp['rows'][] = $row;

                    foreach($row as $col) {
                        if(is_string($col)) $size += strlen($col);
                        else if(is_int($col)) $size += 4;
                        else if(is_float($col)) $size += 8;
                        else $size += 1; // bool, null
                    }
                } while($size <= 10240); // 10kb
                break;

            case 'beginTransaction': // [] => []
                $pdo -> beginTransaction();
                break;

            case 'commit': // [] => []
                $pdo -> commit();
                break;

            case 'rollBack': // [] => []
                $pdo -> rollBack();
                break;

            case 'connect': // [dsn, user, password] => []
                $statements = [];
                $pdo = new PDO($req['dsn'], $req['user'], $req['password']);
                $pdo -> setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
                break;

            case 'exit': // [] => []
                $exit = true;
                break;

            default:
                die(2); // Invalid command
        }
    } catch(Throwable $e) {
        try {
            $pdo -> query('SELECT 1');
            $closed = false;
        } catch(Throwable $_) {
            $closed = true;
        }

        $resp = [
            'success' => false,
            'exception' => get_class($e),
            'code' => $e -> getCode(),
            'message' => $e -> getMessage(),
            'closed' => $closed
        ];
    }

    $buffer = igbinary_serialize($resp);
    echo pack('N', strlen($buffer));
    echo $buffer;

    if($exit)
        break;

    foreach($req['destroy'] as $id)
        unset($statements[$id]);
}

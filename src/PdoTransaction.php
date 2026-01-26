<?php

namespace AppKit\Database\Pdo;

use AppKit\Database\DatabaseTransactionInterface;

class PdoTransaction implements DatabaseTransactionInterface {
    private $pdo;
    private $id;

    private $completed;

    function __construct($pdo, $id) {
        $this -> pdo = $pdo;
        $this -> id = $id;
    }

    function __destruct() {
        $this -> pdo -> _tx_destroy($this -> id);
    }

    public function prepare($query) {
        $this -> ensureActive();

        return $this -> pdo -> _tx_prepare($this -> id, $query);
    }

    public function query($query, $params = []) {
        $this -> ensureActive();

        return $this -> pdo -> _tx_query($this -> id, $query, $params);
    }

    public function commit() {
        $this -> ensureActive();

        $this -> pdo -> _tx_commit($this -> id);
        $this -> completed = 'commited';
    }

    public function rollBack() {
        $this -> ensureActive();

        $this -> pdo -> _tx_rollBack($this -> id);
        $this -> completed = 'rolled back';
    }

    private function ensureActive() {
        if($this -> completed !== null)
            throw new PdoException('Transaction is already ' . $this -> completed);
    }
}

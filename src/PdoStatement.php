<?php

namespace AppKit\Database\Pdo;

use AppKit\Database\DatabaseStatementInterface;

class PdoStatement implements DatabaseStatementInterface {
    private $pdo;
    private $id;

    private $executed;
    private $rowCount;
    private $rows;
    private $end;
    private $cursor;
    private $prefetch;

    function __construct($pdo, $id, $resp) {
        $this -> pdo = $pdo;
        $this -> id = $id;

        $this -> executed = false;

        if(array_key_exists('rowCount', $resp))
            $this -> parseExecuteOrQueryResp($resp);
    }

    function __destruct() {
        $this -> pdo -> _stmt_destroy($this -> id);
    }

    public function execute($params = []) {
        $this -> executed = false;

        $this -> parseExecuteOrQueryResp(
            $this -> pdo -> _stmt_execute($this -> id, $params)
        );

        return $this;
    }

    public function fetch($mode = self::FETCH_ASSOC) {
        $this -> ensureExecuted();
        $this -> maybeFilterPrefetch($mode);

        $row = $this -> rows[$this -> cursor++] ?? null;
        if($row !== null)
            return $row;

        if($this -> end)
            return null;

        $this -> rows = [];
        $this -> cursor = 0;
        $this -> fetchNextBatch($mode);

        return $this -> rows[$this -> cursor++] ?? null;
    }

    public function fetchAll($mode = self::FETCH_ASSOC) {
        $this -> ensureExecuted();
        $this -> maybeFilterPrefetch($mode);

        while(! $this -> end)
            $this -> fetchNextBatch($mode);

        $rows = array_slice($this -> rows, $this -> cursor);

        $this -> rows = [];
        $this -> cursor = 0;

        return $rows;
    }

    public function rowCount() {
        $this -> ensureExecuted();

        return $this -> rowCount;
    }

    private function parseExecuteOrQueryResp($resp) {
        $this -> executed = true;
        $this -> rowCount = $resp['rowCount'];
        $this -> rows = $resp['rows'];
        $this -> end = $resp['end'];
        $this -> cursor = 0;
        $this -> prefetch = true;
    }

    private function ensureExecuted() {
        if(! $this -> executed)
            throw new PdoException('Statement is not executed');
    }

    private function maybeFilterPrefetch($mode) {
        if(! $this -> prefetch)
            return;

        if($mode == self::FETCH_NUM)
            foreach($this -> rows as &$row)
                $row = array_values($row);

        $this -> prefetch = false;
    }

    private function fetchNextBatch($mode) {
        $resp = $this -> pdo -> _stmt_fetch($this -> id, $mode);
        $this -> rows = array_merge($this -> rows, $resp['rows']);
        $this -> end = $resp['end'];
    }
}

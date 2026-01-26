<?php

namespace AppKit\Database\Pdo;

class PdoBackendException extends PdoException {
    private $sqlstate;

    function __construct($message, $sqlstate) {
        parent::__construct($message);
        $this -> sqlstate = $sqlstate;
    }

    public function getSqlstate() {
        return $this -> sqlstate;
    }
}

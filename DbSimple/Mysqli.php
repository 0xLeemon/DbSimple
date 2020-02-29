<?php
/**
 * DbSimple v3.0 (c) 2020 Dmitry Liman
 * https://github.com/DLiman/DbSimple
 *
 * mysqli driver
 */

namespace DbSimple;

/**
 * Database class for MySQL.
 */
class Mysqli extends Database
{
	private $Conn;	// db connection

	/**
	 * constructor(array $parsed_dsn)
	 * Connect to MySQL.
	 */
	public function __construct(array $p)
	{
		if (!extension_loaded('mysqli')) {
			return $this->setLastError("-1", "MySQLi extension is not loaded", "");
		}

		$this->Conn = new \mysqli($p['host'], $p['user'], $p['pass'],
			preg_replace('{^/}s', '', $p['path']),
			isset($p['port']) ? $p['port'] : ini_get('mysqli.default_port')
		);

		$this->resetLastError();

		if ($this->Conn->connect_error) {
			$this->setLastError($this->Conn->connect_errno, $this->Conn->connect_error, "mysqli_connect()");
		}
	}


	public function escape($s, $isIdent = false)
	{
		if (!$isIdent) {
			if (!is_scalar($s)) {
				throw new \Exception("_escape, not a string: " . print_r($s, true));
			}
			return "'{$this->Conn->real_escape_string($s)}'";
		} else {
			$parts = explode('.', $s);
			foreach ($parts as &$p) {
				$p = "`" . str_replace('`', '``', $p) . "`";
			}
			return implode('.', $parts);
		}
	}


	public function transaction($flags = 0)
	{
		$this->logQuery("-- START TRANSACTION ($flags)");
		$this->Conn->begin_transaction($flags);
		return $this->checkError("begin_transaction()");
	}


	protected function newBlob($blobid = null)
	{
		return new MysqliBlob($this, $blobid);
	}


	protected function getBlobFieldNames($Result)
	{
		$blobFields = [];

		foreach ($Result->fetch_fields() as $field) {
			if (stripos($field->type, "BLOB") !== false) {
				$blobFields[] = $field->name;
			}
		}
		return $blobFields;
	}


	protected function getPlaceholderIgnoreRe()
	{
		return '
			"   (?> [^"\\\\]+|\\\\"|\\\\)*    "   |
			\'  (?> [^\'\\\\]+|\\\\\'|\\\\)* \'   |
			`   (?> [^`]+ | ``)*              `   |   # backticks
			/\* .*?                          \*/      # comments
		';
	}


	public function commit()
	{
		$this->logQuery("-- COMMIT");
		$this->Conn->commit();
		return $this->checkError("commit()");
	}


	public function rollback()
	{
		$this->logQuery("-- ROLLBACK");
		$this->Conn->rollback();
		return $this->checkError("rollback()");
	}


	protected function transformQuery(& $sql, $how)
	{
		$result = parent::transformQuery($sql, $how);
		if (false !== $result) {
			return $result;
		}

		// If we also need to calculate total number of found rows...
		switch ($how) {
			// Prepare total calculation (if possible)
			case 'CALC_TOTAL':
				$m = null;
				if (preg_match('/^(\s* SELECT)(.*)/six', $sql, $m)) {
					$sql = $m[1] . ' SQL_CALC_FOUND_ROWS' . $m[2];
				}
				return true;

			// Perform total calculation.
			case 'GET_TOTAL':
				$sql = 'SELECT FOUND_ROWS()';
				// Else use manual calculation.
				// TODO: GROUP BY ... -> COUNT(DISTINCT ...)
				$re = '/^
					(?> -- [^\r\n]* | \s+)*
					(\s* SELECT \s+)                                      #1
					(.*?)                                                 #2
					(\s+ FROM \s+ .*?)                                    #3
						((?:\s+ ORDER \s+ BY \s+ .*?)?)                   #4
						((?:\s+ LIMIT \s+ \S+ \s* (?:, \s* \S+ \s*)? )?)  #5
				$/six';
				$m = null;
				if (preg_match($re, $sql, $m)) {
					$sql = $m[1] . $this->fieldList2Count($m[2]) . " AS C" . $m[3];
					$skipTail = substr_count($m[4] . $m[5], '?');
					if ($skipTail) {
						array_splice($sql, -$skipTail);
					}
				}
				return true;
		}
		return $this->setLastError(-1, "No such transform type: $how", $sql);
	}


	protected function realQuery($query, $args)
	{
		$this->lastQuery = $query;
		$query  = $this->expandPlaceholders($query, $args);
		$result = $this->Conn->query($query);
		if (false === $result) {
			return $this->checkError($query);
		}
		if (!is_object($result)) {
			if (preg_match('/^\s* INSERT \s+/six', $query)) {
				// INSERT queries return generated ID.
				return $this->Conn->insert_id;
			}
			// Non-SELECT queries return number of affected rows, SELECT - resource.
			return $this->Conn->affected_rows;
		}
		return $result;
	}


	protected function fetch($result)
	{
		$row = $result->fetch_assoc();

		if (!$this->checkError("fetch_assoc()")) {
			return false;
		}
		if ($row === false) {
			return null;
		}
		return $row;
	}


	// Returns true if error occured
	protected function checkError($query)
	{
		$this->resetLastError();
		if ($this->Conn->error) {
			$this->setLastError($this->Conn->errno, $this->Conn->error, $query);
		}
		return empty($this->Conn->errno);
	}
}


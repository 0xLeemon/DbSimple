<?php
/**
 * DbSimple v3.0 (c) 2020 Dmitry Liman
 * https://github.com/DLiman/DbSimple
 *
 * Resurrection of DbSimple v2 by DkLab
 *
 * PHP 5.6+, 7+, mysqli, PSR-4, but compatible with legacy projects
 * =====================================================================
 * Originally written by Dmitry Koterov and others
 * (C) Dk Lab, http://en.dklab.ru
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU LGPL v2.1 or later.
 */

namespace DbSimple;

/**
 * Use this constant as placeholder value to skip optional SQL block {...}
 */
define('DBSIMPLE_SKIP', log(0));

/**
 * Names of special columns in result-set which is used
 * as array key (or karent key in forest-based resultsets) in
 * resulting hash.
 */
define('DBSIMPLE_ARRAY_KEY', 'ARRAY_KEY');   // hash-based resultset support
define('DBSIMPLE_PARENT_KEY', 'PARENT_KEY'); // forrest-based resultset support


/**
 * DbSimple factory.
 */
class Generic
{
	/**
	 * DbSimple\Generic connect(mixed $dsn)
	 *
	 * Universal static function to connect ANY database using DSN syntax.
	 * Choose database driver according to DSN. Return new instance
	 * of this driver.
	 */
	public static function connect($dsn)
	{
		// Load database driver and create its instance.
		if (! $parsed = self::parseDSN($dsn)) {
			return null;
		}

		$class = 'DbSimple\\'.ucfirst($parsed['scheme']);
		$object = new $class($parsed);

		if (isset($parsed['ident_prefix'])) {
			$object->setIdentPrefix($parsed['ident_prefix']);
		}
		return $object;
	}


	/**
	 * array parseDSN(mixed $dsn)
	 * Parse a data source name.
	 * See parse_url() for details.
	 */
	public static function parseDSN($dsn)
	{
		if (is_array($dsn)) {
			return $dsn;
		}
		if (! $parsed = @parse_url($dsn)) {
			return null;
		}
		if (!empty($parsed['query'])) {
			parse_str($parsed['query'], $params);
			$parsed += $params;
		}
		$parsed['dsn'] = $dsn;
		return $parsed;
	}
}


/**
 * Base class for all databases.
 *
 * Logger is COMMON for multiple transactions.
 * Error handler is private for each transaction and database.
 */
abstract class Database
{
	/**
	 * mixed select(string $sql [, $arg1] [,$arg2] ...)
	 * Execute query and return the result.
	 */
	public function select($sql, ...$args)
	{
		$total = false;
		return $this->_query($sql, $args, $total);
	}

	/**
	 * mixed selectPage(int &$total, string $sql [, $arg1] [,$arg2] ...)
	 * Execute query and return the result.
	 * Total number of found rows (independent to LIMIT) is returned in $total
	 * (in most cases second query is performed to calculate $total).
	 */
	public function selectPage(&$total, $sql, ...$args)
	{
		$total = true;
		return $this->_query($sql, $args, $total);
	}

	/**
	 * hash selectRow(string $sql [, $arg1] [,$arg2] ...)
	 * Return the first row of query result.
	 * On errors return null and set last error.
	 * If no one row found, return array()! It is useful while debugging,
	 * because PHP DOES NOT generates notice on $row['abc'] if $row === null
	 * or $row === false (but, if $row is empty array, notice is generated).
	 */
	public function selectRow($sql, ...$args)
	{
		$total = false;
		$rows = $this->_query($sql, $args, $total);
		if (!is_array($rows)) {
			return $rows;
		}
		return $rows ? reset($rows) : [];
	}

	/**
	 * array selectCol(string $sql [, $arg1] [,$arg2] ...)
	 * Return the first column of query result as array.
	 */
	public function selectCol($sql, ...$args)
	{
		$total = false;
		$rows = $this->_query($sql, $args, $total);
		if (!is_array($rows)) {
			return $rows;
		}
		$this->shrinkLastArrayDimensionCallback($rows);
		return $rows;
	}

	/**
	 * scalar selectCell(string $sql [, $arg1] [,$arg2] ...)
	 * Return the first cell of the first column of query result.
	 * If no one row selected, return null.
	 */
	public function selectCell($sql, ...$args)
	{
		$total = false;
		$rows = $this->_query($sql, $args, $total);
		if (!is_array($rows)) {
			return $rows;
		}
		if (!$rows) {
			return null;
		}
		$row = reset($rows);
		return is_array($row) ? reset($row) : $row;
	}

	/**
	 * mixed query(string $sql [, $arg1] [,$arg2] ...)
	 * Alias for select(). May be used for INSERT or UPDATE queries.
	 */
	public function query($sql, ...$args)
	{
		$total = false;
		return $this->_query($sql, $args, $total);
	}


	/**
	 * callback setLogger(callback $logger)
	 * Set query logger called before each query is executed.
	 * Returns previous logger.
	 */
	public function setLogger($logger)
	{
		$prev = $this->logger;
		$this->logger = $logger;
		return $prev;
	}

	/**
	 * string setIdentPrefix($prx)
	 * Set identifier prefix used for $_ placeholder.
	 */
	public function setIdentPrefix($prx)
	{
		$old = $this->identPrefix;
		if ($prx !== null) {
			$this->identPrefix = $prx;
		}
		return $old;
	}

	/**
	 * array getStatistics()
	 * Returns various statistical information.
	 */
	public function getStatistics()
	{
		return $this->statistics;
	}


	////////////////////////////////////////////////////////////////////
	//
	// Virtual protected methods
	//

	/**
	 * object newBlob($id)
	 *
	 * Returns new blob object.
	 */
	abstract protected function newBlob($id);

	/**
	 * list getBlobFieldNames($resultResource)
	 * Get list of all BLOB field names in result-set.
	 */
	abstract protected function getBlobFieldNames($result);

	/**
	 * resource realQuery(string $sql, array $args)
	 * Must return:
	 * - For SELECT queries: ID of result-set (PHP resource).
	 * - For other  queries: query status (scalar).
	 * - For error  queries: null (and call _setLastError()).
	 */
	abstract protected function realQuery($sql, $args);

	/**
	 * mixed fetch($resultResource)
	 * Fetch ONE NEXT row from result-set.
	 * Must return:
	 * - For SELECT queries: all the rows of the query (2d arrray).
	 * - For INSERT queries: ID of inserted row.
	 * - For UPDATE queries: number of updated rows.
	 * - For other  queries: query status (scalar).
	 * - For error  queries: null (and call _setLastError()).
	 */
	abstract protected function fetch($result);

	/**
	 * string getPlaceholderIgnoreRe()
	 * Return regular expression which matches ignored query parts.
	 * This is needed to skip placeholder replacement inside comments, constants etc.
	 */
	protected function getPlaceholderIgnoreRe()
	{
		return '';
	}


	////////////////////////////////////////////////////////////////////
	//
	// Private methods
	//

	/**
	 * array _query(string $sql, array $args, &$total)
	 * See realQuery().
	 */
	private function _query($sql, $args, &$total)
	{
		$this->resetLastError();

		// Fetch query attributes.
		$this->attributes = $this->transformQuery($sql, 'GET_ATTRIBUTES');

		// Modify query if needed for total counting.
		if ($total) {
			$this->transformQuery($sql, 'CALC_TOTAL');
		}

		$this->logQuery($sql, $args);

		// Run the query (counting time).
		$qStart = $this->microtime();
		$result = $this->realQuery($sql, $args);
		$fetchTime = $firstFetchTime = 0;

		if (is_resource($result) or is_object($result)) {
			$rows = [];
			// Fetch result row by row.
			$fStart = $this->microtime();
			$row = $this->fetch($result);
			$firstFetchTime = $this->microtime() - $fStart;
			if ($row !== null) {
				$rows[] = $row;
				while ($row = $this->fetch($result)) {
					$rows[] = $row;
				}
			}
			$fetchTime = $this->microtime() - $fStart;
		} else {
			$rows = $result;
		}
		$sqlTime = $this->microtime() - $qStart;

		// Log query statistics.
		$this->logQueryStat($sqlTime, $fetchTime, $firstFetchTime, $rows);

		// Transform resulting rows.
		$result = $this->transformResult($rows);

		// Count total number of rows if needed.
		if (is_array($result) and $total) {
			$this->transformQuery($sql, 'GET_TOTAL');
			$total = $this->selectCell($sql);
		}

		return $result;
	}


	/**
	 * mixed transformQuery(string &$sql, string $how)
	 *
	 * Transform query different way specified by $how.
	 * May return some information about performed transform.
	 */
	protected function transformQuery(&$sql, $how)
	{
		// Common transformations.
		switch ($how) {
			case 'GET_ATTRIBUTES':
				// Extract query attributes.
				$options = [];
				$q = $sql[0];
				$m = null;
				while (preg_match('/^ \s* -- [ \t]+ (\w+): ([^\r\n]+) [\r\n]* /sx', $q, $m)) {
					$options[$m[1]] = trim($m[2]);
					$q = substr($q, strlen($m[0]));
				}
				return $options;
		}
		return false;
	}


	/**
	 * void expandPlaceholders(string $sql, array $args)
	 * Replace placeholders by quoted values.
	 * Modify $sql
	 */
	protected function expandPlaceholders($sql, array $args)
	{
		$cacheCode = null;

		if ($this->logger) {
			// Serialize is much faster than placeholder expansion. So use caching.
			$cacheCode = md5($sql . '|' . serialize($args) . '|' . $this->identPrefix);
			if (isset($this->placeholderCache[$cacheCode])) {
				return $this->placeholderCache[$cacheCode];
			}
		}

		$this->placeholderArgs = array_reverse($args);
		$this->placeholderNoValueFound = false;
		$sql = $this->expandPlaceholdersFlow($sql);

		if ($cacheCode) {
			$this->placeholderCache[$cacheCode] = $sql;
		}
		return $sql;
	}


	/**
	 * Do real placeholder processing.
	 * Imply that all interval variables (_placeholder_*) already prepared.
	 * May be called recurrent!
	 */
	private function expandPlaceholdersFlow($sql)
	{
		$re = '{
			(?>
				# Ignored chunks.
				(?>
					# Comment.
					-- [^\r\n]*
				)
				  |
				(?>
					# DB-specifics.
					' . trim($this->getPlaceholderIgnoreRe()) . '
				)
			)
			  |
			(?>
				# Optional blocks
				\{
					# Use "+" here, not "*"! Else nested blocks are not processed well.
					( (?> (?>[^{}]+)  |  (?R) )* )             #1
				\}
			)
			  |
			(?>
				# Placeholder
				(\?) ( [_dsafno\#]? )                          #2 #3
			)
		}sx';
		$sql = preg_replace_callback(
			$re,
			[$this, 'expandPlaceholdersCallback'],
			$sql
		);
		return $sql;
	}


	/**
	 * string expandPlaceholdersCallback(list $m)
	 * Internal function to replace placeholders (see preg_replace_callback).
	 */
	private function expandPlaceholdersCallback($m)
	{
		// Placeholder.
		if (!empty($m[2])) {
			$type = $m[3];

			// Idenifier prefix.
			if ($type == '_') {
				return $this->identPrefix;
			}

			// Value-based placeholder.
			if (!$this->placeholderArgs) {
				return 'DBSIMPLE_ERROR_NO_VALUE';
			}

			$value = array_pop($this->placeholderArgs);

			// Skip this value?
			if ($value === DBSIMPLE_SKIP) {
				$this->placeholderNoValueFound = true;
				return '';
			}

			// First process guaranteed non-native placeholders.
			switch ($type) {
				case 'a':
					if (!$value) {
						$this->placeholderNoValueFound = true;
					}
					if (is_object($value)) {
						$value = get_object_vars($value);
					}
					if (!is_array($value)) {
						return 'DBSIMPLE_ERROR_VALUE_NOT_ARRAY';
					}

					$parts = [];

					foreach ($value as $k => $v) {
						$v = $v === null ? 'NULL' : $this->escape($v);
						if (!is_int($k)) {
							$k = $this->escape($k, true);
							$parts[] = "$k = $v";
						} else {
							$parts[] = $v;
						}
					}
					return join(', ', $parts);
				case '#':
					// Identifier.
					if (!is_array($value)) {
						return $this->escape($value, true);
					}
					$parts = [];
					foreach ($value as $table => $identifier) {
						if (!is_string($identifier)) {
							return 'DBSIMPLE_ERROR_ARRAY_VALUE_NOT_STRING';
						}
						$parts[] = (!is_int($table) ? $this->escape($table, true) . '.' : '') . $this->escape($identifier, true);
					}
					return join(', ', $parts);
				case 'n':
					// NULL-based placeholder.
					return empty($value) ? 'NULL' : intval($value);
				case 'o':
					// multilevel ORDER BY
					$parts = [];
					foreach ((array)$value as $col => $dir) {
						$parts[] = is_int($col)
							? $this->escape($dir, true)
							: ($this->escape($col, true) . ' ' . (strcasecmp('DESC', $dir) ? 'ASC' : 'DESC'));
					}
					return join(', ', $parts);
			}

			// In non-native mode arguments are quoted.
			if ($value === null) {
				return 'NULL';
			}
			switch ($type) {
				case '':
					if (!is_scalar($value)) {
						return 'DBSIMPLE_ERROR_VALUE_NOT_SCALAR';
					}
					return $this->escape($value);
				case 'd':
					return intval($value);
				case 'f':
					return str_replace(',', '.', floatval($value));
			}
			// By default - escape as string.
			return $this->escape($value);
		}

		// Optional block.
		if (isset($m[1]) && strlen($block=$m[1])) {
			$prev  = @$this->placeholderNoValueFound;
			$block = $this->expandPlaceholdersFlow($block);
			$block = $this->placeholderNoValueFound ? "" : " $block ";
			$this->placeholderNoValueFound = $prev; // recurrent-safe
			return $block;
		}

		// Default: skipped part of the string.
		return $m[0];
	}


	/**
	 * Return microtime as float value.
	 */
	private function microtime()
	{
		$t = explode(" ", microtime());
		return $t[0] + $t[1];
	}


	/**
	 * Convert SQL field-list to COUNT(...) clause
	 * (e.g. 'DISTINCT a AS aa, b AS bb' -> 'COUNT(DISTINCT a, b)').
	 */
	private function fieldList2Count($fields)
	{
		$m = null;
		if (preg_match('/^\s* DISTINCT \s* (.*)/sx', $fields, $m)) {
			$fields = $m[1];
			$fields = preg_replace('/\s+ AS \s+ .*? (?=,|$)/sx', '', $fields);
			return "COUNT(DISTINCT $fields)";
		}
		return 'COUNT(*)';
	}


	/**
	 * array transformResult(list $rows)
	 * Transform resulting rows to various formats.
	 */
	private function transformResult($rows)
	{
		// Process ARRAY_KEY feature.
		if (is_array($rows) and $rows) {
			// Find ARRAY_KEY* AND PARENT_KEY fields in field list.
			$pk = null;
			$ak = [];
			foreach (current($rows) as $fieldName => $dummy) {
				if (0 == strncasecmp($fieldName, DBSIMPLE_ARRAY_KEY, strlen(DBSIMPLE_ARRAY_KEY))) {
					$ak[] = $fieldName;
				} elseif (0 == strncasecmp($fieldName, DBSIMPLE_PARENT_KEY, strlen(DBSIMPLE_PARENT_KEY))) {
					$pk = $fieldName;
				}
			}
			natsort($ak); // sort ARRAY_KEY* using natural comparision

			if ($ak) {
				return $pk === null
					? $this->transformResultToHash($rows, $ak)
					: $this->transformResultToForest($rows, $ak[0], $pk);
			}
		}
		return $rows;
	}


	/**
	 * Converts rowset to key-based array.
	 *
	 * @param array $rows   Two-dimensional array of resulting rows.
	 * @param array $ak     List of ARRAY_KEY* field names.
	 * @return array        Transformed array.
	 */
	private function transformResultToHash($rows, $arrayKeys)
	{
		$arrayKeys = (array) $arrayKeys;
		$result = [];
		foreach ($rows as $row) {
			// Iterate over all of ARRAY_KEY* fields and build array dimensions.
			$current =& $result;
			foreach ($arrayKeys as $ak) {
				$key = $row[$ak];
				unset($row[$ak]); // remove ARRAY_KEY* field from result row
				if ($key !== null) {
					$current =& $current[$key];
				} else {
					// IF ARRAY_KEY field === null, use array auto-indices.
					$tmp = [];
					$current[] =& $tmp;
					$current =& $tmp;
					unset($tmp); // we use жtmp, because don't know the value of auto-index
				}
			}
			$current = count($row) == 1 ? reset($row) : $row; // save the row in last dimension
		}
		return $result;
	}


	/**
	 * Converts rowset to the forest.
	 *
	 * @param array $rows       Two-dimensional array of resulting rows.
	 * @param string $idName    Name of ID field.
	 * @param string $pidName   Name of PARENT_ID field.
	 * @return array            Transformed array (tree).
	 */
	private function transformResultToForest($rows, $idName, $pidName)
	{
		$children = []; // children of each ID
		$ids = [];
		// Collect who are children of whom.
		foreach ($rows as $i => $r) {
			$row =& $rows[$i];
			$id = $row[$idName];
			if ($id === null) {
				// Rows without an ID are totally invalid and makes the result tree to
				// be empty (because PARENT_ID = null means "a root of the tree"). So
				// skip them totally.
				continue;
			}
			$pid = $row[$pidName];
			if ($id == $pid) $pid = null;
			$children[$pid][$id] =& $row;
			if (!isset($children[$id])) $children[$id] = [];
			$row['childNodes'] =& $children[$id];
			$ids[$id] = true;
		}
		// Root elements are elements with non-found PIDs.
		$forest = [];
		foreach ($rows as $i => $r) {
			$row =& $rows[$i];
			$id = $row[$idName];
			$pid = $row[$pidName];
			if ($pid == $id) {
				$pid = null;
			}
			if (!isset($ids[$pid])) {
				$forest[$row[$idName]] =& $row;
			}
			unset($row[$idName]);
			unset($row[$pidName]);
		}
		return $forest;
	}


	/**
	 * Replaces the last array in a multi-dimensional array $V by its first value.
	 * Used for selectCol(), when we need to transform (N+1)d resulting array
	 * to Nd array (column).
	 */
	private function shrinkLastArrayDimensionCallback(&$v)
	{
		if (!$v) return;
		reset($v);
		if (!is_array($firstCell = current($v))) {
			$v = $firstCell;
		} else {
			array_walk($v, [$this, 'shrinkLastArrayDimensionCallback']);
		}
	}


	/**
	 * void logQuery($sql, $args, $noTrace = false)
	 * Must be called on each query.
	 * If $noTrace is true, library caller is not solved (speed improvement).
	 */
	protected function logQuery($sql, array $args = [], $noTrace = false)
	{
		if ($this->logger) {
			$sql = $this->expandPlaceholders($sql, $args);
			return call_user_func($this->logger, $this, $sql, $noTrace ? null : $this->findLibraryCaller());
		}
	}


	/**
	 * void logQueryStat($sqlTime, $fetchTime, $firstFetchTime, $rows)
	 * Log information about performed query statistics.
	 */
	protected function logQueryStat($sqlTime, $fetchTime, $firstFetchTime, $rows)
	{
		// Always increment counters.
		$this->statistics['time'] += $sqlTime;
		$this->statistics['count']++;

		// If no logger, economize CPU resources and actually log nothing.
		if (!$this->logger) return;

		$dt = round($sqlTime * 1000);
		$firstFetchTime = round($firstFetchTime * 1000);
		$tailFetchTime = round($fetchTime * 1000) - $firstFetchTime;
		$log = "  -- ";
		if ($firstFetchTime + $tailFetchTime) {
			$log = sprintf("  -- %d ms = %d+%d".($tailFetchTime? "+%d" : ""), $dt, $dt-$firstFetchTime-$tailFetchTime, $firstFetchTime, $tailFetchTime);
		} else {
			$log = sprintf("  -- %d ms", $dt);
		}
		$log .= "; returned ";

		if (!is_array($rows)) {
			$log .= $this->escape($rows);
		} else {
			$detailed = null;
			if (count($rows) == 1) {
				$len = 0;
				$values = array();
				foreach ($rows[0] as $k => $v) {
					$len += strlen($v);
					if ($len > $this->MAX_LOG_ROW_LEN) {
						break;
					}
					$values[] = $v === null? 'NULL' : $this->escape($v);
				}
				if ($len <= $this->MAX_LOG_ROW_LEN) {
					$detailed = "(" . preg_replace("/\r?\n/", "\\n", join(', ', $values)) . ")";
				}
			}
			if ($detailed) {
				$log .= $detailed;
			} else {
				$log .= count($rows). " row(s)";
			}
		}

		$this->logQuery($log, [], true);
	}

	// Identifiers prefix (used for ?_ placeholder).
	private $identPrefix = '';

	// Queries statistics.
	private $statistics = [
		'time'  => 0,
		'count' => 0,
	];

	private $logger = null;
	private $placeholderArgs, $placeholderCache = [];
	private $placeholderNoValueFound;

	/**
	 * When string representation of row (in characters) is greater than this,
	 * row data will not be logged.
	*/
	public $MAX_LOG_ROW_LEN = 128;


	////////////////////////////////////////////////////////////////////
	//
	// Errors handling
	//

	private $error = null;
	private $errmsg = null;
	private $errorHandler = null;

	/**
	 * void resetLastError()
	 * Reset the last error. Must be called on correct queries.
	 */
	protected function resetLastError()
	{
	}

	/**
	 * void setLastError(int $code, string $message, string $sql)
	 * Fill $this->error property with error information. Error context
	 * (code initiated the query outside DbSimple) is assigned automatically.
	 */
	protected function setLastError($code, $msg, $sql)
	{
		$context = "unknown";
		if ($t = $this->findLibraryCaller()) {
			$context = (isset($t['file']) ? $t['file'] : '?') . ' line ' . (isset($t['line']) ? $t['line'] : '?');
		}
		$this->error = [
			'code'    => $code,
			'message' => rtrim($msg),
			'query'   => $sql,
			'context' => $context,
		];
		$this->errmsg = rtrim($msg) . ($context ? " at $context" : "");

		$this->logQuery("  -- error #{$code}: ".preg_replace('/(\r?\n)+/s', ' ', $this->errmsg));

		if (is_callable($this->errorHandler)) {
			call_user_func($this->errorHandler, $this->errmsg, $this->error);
		}
		return false;
	}


	/**
	 * callback setErrorHandler(callback $handler)
	 * Set new error handler called on database errors.
	 * Handler gets 3 arguments:
	 * - error message
	 * - full error context information (last query etc.)
	 */
	public function setErrorHandler($handler)
	{
		$prev = $this->errorHandler;
		$this->errorHandler = $handler;
		// In case of setting first error handler for already existed
		// error - call the handler now (usual after connect()).
		if (!$prev and $this->error) {
			$handler($this->errmsg, $this->error);
		}
		return $prev;
	}


	/**
	 * array findLibraryCaller()
	 * Return an entry of stacktrace before calling first library method
	 * Used in debug purposes (query logging etc.)
	 *
	 * NB: the algo is mostly heuristic, so don't forget to test it when
	 * call chain changed
	 */
	public function findLibraryCaller()
	{
		$trace = debug_backtrace();
		$prev = array_pop($trace);
		while ($cur = array_pop($trace)) {
			if ($cur['class'] === 'DbSimple\\Database' and ($cur['function'] === '_query' or $cur['function'] === 'logQuery')) {
				return $prev;
			} else {
				$prev = $cur;
			}
		}
		return null;
	}

}


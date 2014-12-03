<?php

class DatabaseMysqli extends DatabaseMysql {
	/**
	 * @var mysqli
	 */
	protected $mConn = null;

	public function getType() {
		return 'mysqli';
	}

	protected function doQuery( $sql ) {
		if( $this->bufferResults() ) {
			$ret = $this->mConn->query( $sql );
		} else {
			// FIXME: Needs to be unbuffered
			$ret = $this->mConn->query( $sql );
		}
		return $ret;
	}

	/**
	 * Open a connection to the database. Usually aborts on failure
	 *
	 * @param $server String: database server host
	 * @param $user String: database user name
	 * @param $password String: database user password
	 * @param $dbName String: database name
	 * @return bool
	 * @throws DBConnectionError
	 */
	function open( $server, $user, $password, $dbName ) {
		global $wgAllDBsAreLocalhost;
		wfProfileIn( __METHOD__ );

		# Load mysqli
		wfDl( 'mysqli' );

		if ( !class_exists( 'mysqli' ) ) {
			throw new DBConnectionError( $this, "MySQLi functions missing, have you compiled PHP with the --with-mysqli option?\n" );
		}

		# Debugging hack -- fake cluster
		if ( $wgAllDBsAreLocalhost ) {
			$realServer = 'localhost';
		} else {
			$realServer = $server;
		}
		if ( $this->mFlags & DBO_PERSISTENT ) {
			$realServer = 'p:' . $realServer;
		}
		$this->close();
		$this->mServer = $server;
		$this->mUser = $user;
		$this->mPassword = $password;
		$this->mDBname = $dbName;

		wfProfileIn( "dbconnect-$server" );

		// TODO: Attempt counts
		$numAttempts = 2;

		$this->installErrorHandler();
		for ( $i = 0; $i < $numAttempts && !$this->mConn; $i++ ) {
			if ( $i > 1 ) {
				usleep( 1000 );
			}
			$this->mConn = new mysqli($realServer, $user, $password, $dbName);
		}
		$phpError = $this->restoreErrorHandler();
		# Always log connection errors
		if ( !$this->mConn ) {
			$error = $this->lastError();
			if ( !$error ) {
				$error = $phpError;
			}
			wfLogDBError( "Error connecting to {$this->mServer}: $error\n" );
			wfDebug( "DB connection error\n" );
			wfDebug( "Server: $server, User: $user, Password: " .
				substr( $password, 0, 3 ) . "..., error: " . mysql_error() . "\n" );
		}

		wfProfileOut("dbconnect-$server");

		$success = (bool)$this->mConn;

		if ( $success ) {
			$version = $this->getServerVersion();
			if ( version_compare( $version, '4.1' ) >= 0 ) {
				// Tell the server we're communicating with it in UTF-8.
				// This may engage various charset conversions.
				global $wgDBmysql5;
				if( $wgDBmysql5 ) {
					$this->query( 'SET NAMES utf8', __METHOD__ );
				} else {
					$this->query( 'SET NAMES binary', __METHOD__ );
				}
				// Set SQL mode, default is turning them all off, can be overridden or skipped with null
				global $wgSQLMode;
				if ( is_string( $wgSQLMode ) ) {
					$mode = $this->addQuotes( $wgSQLMode );
					$this->query( "SET sql_mode = $mode", __METHOD__ );
				}
			}

			// Turn off strict mode if it is on
		} else {
			$this->reportConnectionError( $phpError );
		}

		$this->mOpened = $success;
		wfProfileOut( __METHOD__ );
		return $success;
	}

	public function close() {
		$this->mOpened = false;
		if ( $this->mConn ) {
			if ( $this->trxLevel() ) {
				$this->commit();
			}
			$this->mConn->close();
		} else {
			return true;
		}
	}

	protected function unwrapResult( $res ) {
		if ( $res instanceof ResultWrapper ) {
			$res = $res->result;
		}
		return $res;
	}

	/**
	 * Frees a MySQLi result from memory
	 *
	 * @param $res mysqli_result
	 */
	public function freeResult( $res ) {
		$res = $this->unwrapResult( $res );

		// TODO: Is surpressing warnings the right thing here?
		wfSuppressWarnings();
		$res->free_result();
		wfRestoreWarnings();
	}

	/**
	 * Fetch the next row from the given result object, in object form.
	 * Fields can be retrieved with $row->fieldname, with fields acting like
	 * member variables.
	 *
	 * @param $res SQL result object as returned from DatabaseBase::query(), etc.
	 * @return Row object
	 * @throws DBUnexpectedError Thrown if the database returns an error
	 */
	function fetchObject( $res ) {
		$res = $this->unwrapResult( $res );
		wfSuppressWarnings();
		$row = $res->fetch_object();
		wfRestoreWarnings();
		if( $this->lastErrno() ) {
			throw new DBUnexpectedError( $this, 'Error in fetchObject(): ' . htmlspecialchars( $this->lastError() ) );
		}

		# In mysql, fetch_object would return false when there were no more
		# rows in the resultset. This behaviour is different in mysqli and it
		# returns null instead, which breaks Iterators and other code which
		# expects false.
		if ( $row === null ) {
			$row = false;
		}

		return $row;
	}

	/**
	 * Fetch the next row from the given result object, in associative array
	 * form.  Fields are retrieved with $row['fieldname'].
	 *
	 * @param $res ResultWrapper result object as returned from DatabaseBase::query(), etc.
	 * @return Row object
	 * @throws DBUnexpectedError Thrown if the database returns an error
	 */
	function fetchRow( $res ) {
		$res = $this->unwrapResult( $res );
		wfSuppressWarnings();
		$row = $res->fetch_array();
		wfRestoreWarnings();
		if ( $this->lastErrno() ) {
			throw new DBUnexpectedError( $this, 'Error in fetchRow(): ' . htmlspecialchars( $this->lastError() ) );
		}
		return $row;
	}

	/**
	 * Get the number of rows in a result object
	 *
	 * @param $res Mixed: A SQL result
	 * @return int
	 */
	function numRows( $res ) {
		$res = $this->unwrapResult( $res );
		wfSuppressWarnings();
		$n = $res->num_rows;
		wfRestoreWarnings();
		if( $this->lastErrno() ) {
			throw new DBUnexpectedError( $this, 'Error in numRows(): ' . htmlspecialchars( $this->lastError() ) );
		}
		return $n;
	}

	/**
	 * Get the number of fields in a result object
	 * @see http://www.php.net/mysql_num_fields
	 *
	 * @param $res Mixed: A SQL result
	 * @return int
	 */
	function numFields( $res ) {
		$res = $this->unwrapResult( $res );
		return $res->field_count;
	}

	/**
	 * Get a field name in a result object
	 * @see http://www.php.net/mysql_field_name
	 *
	 * @param $res mysqli_result A SQL result
	 * @param $n Integer
	 * @return string
	 */
	function fieldName( $res, $n ) {
		$res = $this->unwrapResult( $res );
		$ok = $res->field_seek( $n );
		if ( $ok ) {
			return $res->fetch_field()->name;
		} else {
			return false;
		}
	}

	/**
	 * Get the inserted value of an auto-increment row
	 *
	 * The value inserted should be fetched from nextSequenceValue()
	 *
	 * Example:
	 * $id = $dbw->nextSequenceValue('page_page_id_seq');
	 * $dbw->insert('page',array('page_id' => $id));
	 * $id = $dbw->insertId();
	 *
	 * @return int
	 */
	function insertId() {
		return $this->mConn->insert_id;
	}

	/**
	 * Change the position of the cursor in a result object
	 * @see http://www.php.net/mysql_data_seek
	 *
	 * @param $res Mixed: A SQL result
	 * @param $row Mixed: Either MySQL row or ResultWrapper
	 */
	function dataSeek( $res, $row ) {
		$res = $this->unwrapResult( $res );
		return $res->data_seek( $row );
	}

	/**
	 * Get the last error number
	 * @see http://www.php.net/mysql_errno
	 *
	 * @return int
	 */
	function lastErrno() {
		if ( $this->mConn ) {
			return $this->mConn->errno;
		} else {
			return mysqli_connect_errno();
		}
	}

	/**
	 * Get a description of the last error
	 * @see http://www.php.net/mysql_error
	 *
	 * @return string
	 */
	function lastError() {
		if ( $this->mConn ) {
			# Even if it's non-zero, it can still be invalid
			wfSuppressWarnings();
			$error = $this->mConn->error;
			if ( !$error ) {
				// TODO: Come back to this later
			}
			wfRestoreWarnings();
		} else {
			$error = mysqli_connect_error();
		}
		if( $error ) {
			$error .= ' (' . $this->mServer . ')';
		}
		return $error;
	}

	/**
	 * mysql_fetch_field() wrapper
	 * Returns false if the field doesn't exist
	 *
	 * @param $table string: table name
	 * @param $field string: field name
	 *
	 * @return Field
	 */
	function fieldInfo( $table, $field ) {
		$table = $this->tableName( $table );
		$res = $this->query( "SELECT * FROM $table LIMIT 1", __METHOD__, true );
		if ( !$res ) {
			return false;
		}
		$n = $this->numFields( $res );
		for( $i = 0; $i < $n; $i++ ) {
			$meta = $res->result->fetch_field_direct( $n );
			if( $field == $meta->name ) {
				// TODO: Check that this works okay
				return new MySQLField($meta);
			}
		}
		return false;
	}

	/**
	 * Get the number of rows affected by the last write query
	 * @see http://www.php.net/mysql_affected_rows
	 *
	 * @return int
	 */
	function affectedRows() {
		return $this->mConn->affected_rows;
	}

	public function selectDB( $db ) {
		$this->mDBname = $db;
		return $this->mConn->select_db( $db );
	}

	/**
	 * Wrapper for addslashes()
	 *
	 * @param $s string: to be slashed.
	 * @return string: slashed string.
	 */
	function strencode( $s ) {
		return $this->mConn->real_escape_string( $s );
	}

	function ping() {
		$ping = $this->mConn->ping();
		if ( $ping ) {
			return true;
		}

		$this->mConn->close();
		$this->mOpened = false;
		$this->mConn = false;
		$this->open( $this->mServer, $this->mUser, $this->mPassword, $this->mDBname );
		return true;
	}

	/**
	 * A string describing the current software version, like from
	 * mysql_get_server_info().
	 *
	 * @return string: Version information from the database server.
	 */
	function getServerVersion() {
		return "5.6.21";
	}
}
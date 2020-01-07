use "ponytest"
use "fileExt"
use "stringExt"
use "json"

use "lib:sqlite3"

// Based on: https://github.com/joncfoo/pony-sqlite
use @sqlite3_libversion[Pointer[U8]]()
use @sqlite3_open_v2[SqliteResultCode](
  file_name: Pointer[U8] tag,
  connection: Pointer[Pointer[_Connection]],
  flags: I32,
  vfs: Pointer[U8] tag
)
use @sqlite3_close_v2[SqliteResultCode](connection: Pointer[_Connection] tag)
use @sqlite3_errstr[Pointer[U8]](rc: SqliteResultCode)

use @sqlite3_prepare_v3[SqliteResultCode](
  connection: Pointer[_Connection] tag,
  sql: Pointer[U8] tag,
  sql_len: I32,
  prepare_flags: SqlitePrepareFlag,
  statement: Pointer[Pointer[_Statement]],
  sql_tail: Pointer[Pointer[U8]]
)
use @sqlite3_step[SqliteResultCode](statement: Pointer[_Statement] tag)
use @sqlite3_reset[SqliteResultCode](statement: Pointer[_Statement] tag)
use @sqlite3_finalize[SqliteResultCode](statement: Pointer[_Statement] tag)

use @sqlite3_bind_parameter_count[I32](statement: Pointer[_Statement] tag)
use @sqlite3_bind_parameter_index[I32](statement: Pointer[_Statement] tag, name: Pointer[U8] tag)
use @sqlite3_bind_parameter_name[Pointer[U8]](statement: Pointer[_Statement] tag, column: I32)
use @sqlite3_bind_null[SqliteResultCode](statement: Pointer[_Statement] tag, column: I32)
use @sqlite3_bind_double[SqliteResultCode](statement: Pointer[_Statement] tag, column: I32, value: F64)
use @sqlite3_bind_int[SqliteResultCode](statement: Pointer[_Statement] tag, column: I32, value: I32)
use @sqlite3_bind_int64[SqliteResultCode](statement: Pointer[_Statement] tag, column: I32, value: I64)
use @sqlite3_bind_text[SqliteResultCode](statement: Pointer[_Statement] tag, column: I32, value: Pointer[U8] tag, length: I32, destructor: @{(): None})
use @sqlite3_bind_text64[SqliteResultCode](statement: Pointer[_Statement] tag, column: I32, value: Pointer[U8] tag, length: U64, destructor: @{(): None}, encoding: U8)
use @sqlite3_bind_blob[SqliteResultCode](statement: Pointer[_Statement] tag, column: I32, value: Pointer[U8] tag, length: I32, destructor: @{(): None})
use @sqlite3_bind_blob64[SqliteResultCode](statement: Pointer[_Statement] tag, column: I32, value: Pointer[U8] tag, length: U64, destructor: @{(): None})
use @sqlite3_bind_zeroblob[SqliteResultCode](statement: Pointer[_Statement] tag, column: I32, length: I32)
use @sqlite3_bind_zeroblob64[SqliteResultCode](statement: Pointer[_Statement] tag, column: I32, length: U64)

use @sqlite3_column_count[I32](statement: Pointer[_Statement] tag)
use @sqlite3_column_name[Pointer[U8]](statement: Pointer[_Statement] tag, column: I32)
use @sqlite3_column_type[SqliteDataType](statement: Pointer[_Statement] tag, column: I32)
use @sqlite3_column_blob[Pointer[U8]](statement: Pointer[_Statement] tag, column: I32)
use @sqlite3_column_double[F64](statement: Pointer[_Statement] tag, column: I32)
use @sqlite3_column_int[I32](statement: Pointer[_Statement] tag, column: I32)
use @sqlite3_column_int64[I64](statement: Pointer[_Statement] tag, column: I32)
use @sqlite3_column_text[Pointer[U8]](statement: Pointer[_Statement] tag, column: I32)
use @sqlite3_column_bytes[I32](statement: Pointer[_Statement] tag, column: I32)

primitive _Connection
primitive _Statement

type SqliteOpenFlag is I32
type SqliteResultCode is I32
type SqlitePrepareFlag is U32
type SqliteDataType is I32

primitive SQL3
	fun open_readonly():       SqliteOpenFlag => 0x00000001  	/* Ok for sqlite3_open_v2() */
	fun open_readwrite():      SqliteOpenFlag => 0x00000002  	/* Ok for sqlite3_open_v2() */
	fun open_create():         SqliteOpenFlag => 0x00000004  	/* Ok for sqlite3_open_v2() */
	fun open_deleteonclose():  SqliteOpenFlag => 0x00000008  	/* VFS only */
	fun open_exclusive():      SqliteOpenFlag => 0x00000010  	/* VFS only */
	fun open_autoproxy():      SqliteOpenFlag => 0x00000020  	/* VFS only */
	fun open_uri():            SqliteOpenFlag => 0x00000040  	/* Ok for sqlite3_open_v2() */
	fun open_memory():         SqliteOpenFlag => 0x00000080  	/* Ok for sqlite3_open_v2() */
	fun open_main_db():        SqliteOpenFlag => 0x00000100  	/* VFS only */
	fun open_temp_db():        SqliteOpenFlag => 0x00000200  	/* VFS only */
	fun open_transient_db():   SqliteOpenFlag => 0x00000400  	/* VFS only */
	fun open_main_journal():   SqliteOpenFlag => 0x00000800  	/* VFS only */
	fun open_temp_journal():   SqliteOpenFlag => 0x00001000  	/* VFS only */
	fun open_subjournal():     SqliteOpenFlag => 0x00002000  	/* VFS only */
	fun open_master_journal(): SqliteOpenFlag => 0x00004000  	/* VFS only */
	fun open_nomutex():        SqliteOpenFlag => 0x00008000  	/* Ok for sqlite3_open_v2() */
	fun open_fullmutex():      SqliteOpenFlag => 0x00010000  	/* Ok for sqlite3_open_v2() */
	fun open_sharedcache():    SqliteOpenFlag => 0x00020000  	/* Ok for sqlite3_open_v2() */
	fun open_privatecache():   SqliteOpenFlag => 0x00040000  	/* Ok for sqlite3_open_v2() */
	fun open_wal():            SqliteOpenFlag => 0x00080000  	/* VFS only */
	
    fun prepare_persistent(): SqlitePrepareFlag => 1			/* A hint to the query planner that the prepared statement will be retained for a long time and probably reused many times. */
    fun prepare_no_vtab(): SqlitePrepareFlag =>	4				/* The SQLITE_PREPARE_NO_VTAB flag causes the SQL compiler to return an error (error rc SQLITE_ERROR) if the statement uses any virtual tables. */
	
    fun data_integer(): SqliteDataType => 1						/* Integer data-type */
    fun data_float(): SqliteDataType => 2						/* Float data-type */
    fun data_text(): SqliteDataType => 3						/* Text data-type */
    fun data_blob(): SqliteDataType => 4						/* Blob data-type */
    fun data_null(): SqliteDataType => 5						/* Null data-type */
	
    fun result_ok(): SqliteResultCode => 0 						/* Successful result */
    fun result_err(): SqliteResultCode => 1 					/* Generic error */
    fun result_internal(): SqliteResultCode => 2 				/* Internal logic error in SQLite */
    fun result_perm(): SqliteResultCode => 3 					/* Access permission denied */
    fun result_abort(): SqliteResultCode => 4 					/* Callback routine requested an abort */
    fun result_busy(): SqliteResultCode => 5					/* The database file is locked */
    fun result_locked(): SqliteResultCode => 6 					/* A table in the database is locked */
    fun result_nomem(): SqliteResultCode => 7 					/* A malloc() failed */
    fun result_readonly(): SqliteResultCode => 8 				/* Attempt to write a readonly database */
    fun result_interrupt(): SqliteResultCode => 9 				/* Operation terminated by `sqlite3_interrupt()` */
    fun result_ioerr(): SqliteResultCode =>	10					/* Some kind of disk I/O error occurred */
    fun result_corrupt(): SqliteResultCode => 11				/* The database disk image is malformed */
    fun result_notfound(): SqliteResultCode => 12				/* Unknown opcode in `sqlite3_file_control()` */
    fun result_full(): SqliteResultCode => 13					/* Insertion failed because database is full */
    fun result_cantopen(): SqliteResultCode => 14				/* Unable to open the database file */
    fun result_protocol(): SqliteResultCode => 15				/* Database lock protocol error */
    fun result_empty(): SqliteResultCode => 16					/* Internal use only */
    fun result_schema(): SqliteResultCode => 17					/* The database schema changed */
    fun result_toobig(): SqliteResultCode => 18					/* String or BLOB exceeds size limit */
    fun result_constraint(): SqliteResultCode => 19				/* Abort due to constraint violation */
    fun result_mismatch(): SqliteResultCode => 20				/* Data type mismatch */
    fun result_misuse(): SqliteResultCode => 21					/* Library used incorrectly */
    fun result_nolfs(): SqliteResultCode =>	22					/* Uses OS features not supported on host */
    fun result_auth(): SqliteResultCode => 23					/* Authorization denied */
    fun result_format(): SqliteResultCode => 24					/* Not used */
    fun result_range(): SqliteResultCode => 25					/* 2nd parameter to sqlite3_bind out of range */
    fun result_notadb(): SqliteResultCode => 26					/* File opened that is not a database file */
    fun result_notice(): SqliteResultCode => 27					/* Notifications from `sqlite3_log()` */
    fun result_warning(): SqliteResultCode => 28				/* Warnings from `sqlite3_log()` */
    fun result_row(): SqliteResultCode => 100					/* `sqlite3_step()` has another row ready */
    fun result_done(): SqliteResultCode => 101					/* `sqlite3_step()` has finished executing */


interface Sqlite3Delegate
	


actor Sqlite3
	"""
	Provide a generic interface for interacting with Sqlite3 databases.
	Note that sqlite3 is read concurrent but not write concurrent, so optimal uses would mean that
	individual actors access their own separate databases.
	"""
	
	let isEmpty:Bool
	let open_flags:SqliteOpenFlag = (SQL3.open_readwrite() or SQL3.open_create())
	
	var connection:Pointer[_Connection] = Pointer[_Connection]
	
	new empty() =>
		isEmpty = true
	
	new create() =>
		isEmpty = true
	
	new memory()? =>
		isEmpty = false
	    let rc = @sqlite3_open_v2( ":memory:".cstring(), addressof connection, open_flags, Pointer[U8].create() )
		if (rc != SQL3.result_ok()) then error end
	
	new file(pathToFile:String)? =>
		isEmpty = false
	    let rc = @sqlite3_open_v2( pathToFile.cstring(), addressof connection, open_flags, Pointer[U8].create() )
		if (rc != SQL3.result_ok()) then error end
	
	new errorTest()? =>
		isEmpty = true
		error
	

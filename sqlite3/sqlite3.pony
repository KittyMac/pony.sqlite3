use "stringExt"
use "collections"

use "lib:sqlite3"

// MARK: ---------------------------- CLASS SQLITE3 ----------------------------

type SQL3Connection is Sqlite3
type SqliteResultCode is U32

class PonySqlite3
  """
  Provide a generic interface for interacting with Sqlite3 databases.
  Note that sqlite3 is read concurrent but not write concurrent, so optimal uses would mean that
  individual actors access their own separate databases.
  """
  
  let isEmpty:Bool
  let open_flags:U32 = (Sqlite.open_readwrite() or Sqlite.open_create())
  
  var connection:Pointer[SQL3Connection] tag = Pointer[SQL3Connection]
  
  new empty() =>
    isEmpty = true
  
  new create() =>
    isEmpty = true
  
  new memory()? =>
    isEmpty = false
    let rc = @sqlite3_open_v2( ":memory:".cstring(), addressof connection, open_flags, Pointer[U8].create() )
    if (rc != Sqlite.ok()) then 
      close()
      error
    end
  
  new file(pathToFile:String)? =>
    isEmpty = false
    var rc = Sqlite.ok()
    if (StringExt.startswith(pathToFile, ":memory:")) then
      rc = @sqlite3_open_v2( ":memory:".cstring(), addressof connection, open_flags, Pointer[U8].create() )
    else
      rc = @sqlite3_open_v2( pathToFile.cstring(), addressof connection, open_flags, Pointer[U8].create() )
    end
    if (rc != Sqlite.ok()) then
      close()
      error
    end
  
  fun ref close():SqliteResultCode =>
    if connection.is_null() == false then
      let result = @sqlite3_close_v2(connection)
      connection = Pointer[SQL3Connection]
      return result
    end
    Sqlite.ok()
    
  fun _final() =>
    if connection.is_null() == false then
      @sqlite3_close_v2(connection)
    end
  
  fun version(): String iso^ =>
    recover String.copy_cstring(@sqlite3_libversion()) end
  
  fun result_description(rc:SqliteResultCode): String iso^ =>
    recover String.copy_cstring(@sqlite3_errstr(rc)) end
  
  fun ref error_message(): String ref =>
    String.copy_cstring(@sqlite3_errmsg(connection))
  
  fun ref query(sqlThing:(String box | SqliteSqlStatement)):SqliteQueryIter? =>
    
    match sqlThing
    | let sqlString:String box =>
      var stmt:Pointer[Sqlite3Stmt] tag = Pointer[Sqlite3Stmt]
      var sql_tail_unused:Pointer[U8] tag = Pointer[U8]
      
      var rc = @sqlite3_prepare_v2(connection, sqlString.cpointer(), sqlString.size().u32()+1, addressof stmt, addressof sql_tail_unused)
      if (rc != Sqlite.ok()) or stmt.is_null() then
        close()
        error
      end
      
      SqliteQueryIter(stmt)
    | let sqlStmt:SqliteSqlStatement =>
      
      SqliteQueryIter(sqlStmt.stmt)
    end
        
    
  fun ref exec(sqlString:String)? =>
    // sqlite3_exec[U32](arg2:Pointer[Sqlite3] tag, sql:Pointer[U8] tag, callback:Pointer[None] tag, arg3:Pointer[None] tag, errmsg:Pointer[Pointer[U8] tag] tag)
    var errmsg:Pointer[U8] tag = Pointer[U8]
    let rc = @sqlite3_exec[SqliteResultCode](connection, sqlString.cstring(), Pointer[U8], Pointer[U8], addressof errmsg)
    if rc != Sqlite.ok() then
      close()
      error
    end
  
  fun ref branch(sqlString:(String box | SqliteSqlStatement)):Bool? =>
    // run the query, if num rows > 0 then return true
    let s = sqlString.string()
    if s.contains("INSERT") or s.contains("UPDATE") or s.contains("DELETE") then
      query(sqlString)?.finish()?
      return @sqlite3_changes(connection) > 0
    else
      return query(sqlString)?.has_next()
    end
  
  fun ref printAll(sqlString:(String box | SqliteSqlStatement))? =>
    // run the query, if num rows > 0 then return true
    @fprintf[I32](@pony_os_stdout[Pointer[U8]](), "=========================\n".cstring())
    @fprintf[I32](@pony_os_stdout[Pointer[U8]](), "> %s\n".cstring(), sqlString.string().cstring())
    for row in query(sqlString)? do
      try
        for i in Range[U32](0, 10) do
          @fprintf[I32](@pony_os_stdout[Pointer[U8]](), "%s, ".cstring(), row.string(i)?.cstring())
        end
      end
      @fprintf[I32](@pony_os_stdout[Pointer[U8]](), "\n".cstring())
    end
    @fprintf[I32](@pony_os_stdout[Pointer[U8]](), "=========================\n".cstring())
  
  fun ref beginTransaction()? =>
    exec("BEGIN TRANSACTION")?
  
  fun ref endTransaction()? =>
    exec("END TRANSACTION")?
  
  fun ref sql(sqlString:String):SqliteSqlStatement? =>
    SqliteSqlStatement(connection, sqlString)?

// MARK: ---------------------------- CLASS SQLITESQLSTATEMENT ----------------------------

class SqliteSqlStatement
  let sql:String
  var stmt:Pointer[Sqlite3Stmt] tag
  var bind_index:U32 = 1
  
  fun string():String =>
    sql
  
  new create(connection:Pointer[SQL3Connection] tag, sql':String)? =>
    stmt = Pointer[Sqlite3Stmt]
    sql = sql'
    
    var sql_tail_unused:Pointer[U8] tag = Pointer[U8]
    var rc = @sqlite3_prepare_v2(connection, sql.cpointer(), sql.size().u32()+1, addressof stmt, addressof sql_tail_unused)
    if (rc != Sqlite.ok()) or stmt.is_null() then
      error
    end
  
    fun @_noop() =>
      None
  
  fun ref bind(value:(None | F64 | I32 | I64 | String box | Array[U8] box)) =>
    match value
    | None =>
      @sqlite3_bind_null(stmt, bind_index)
    | let v: F64 =>
      @sqlite3_bind_double(stmt, bind_index, v)
    | let v: I32 =>
      @sqlite3_bind_int(stmt, bind_index, v.u32())
    | let v: I64 =>
      @sqlite3_bind_int64(stmt, bind_index, v)
    | let v: String box =>
      let length = v.size().u64()
      if length <= I32.max_value().u64() then
        @sqlite3_bind_text(stmt, bind_index, v.cpointer(), v.size().u32(), addressof this._noop)
      else
        let utf8: U8 = 1
        @sqlite3_bind_text64(stmt, bind_index, v.cpointer(), length, addressof this._noop, utf8)
      end
    | let v: Array[U8] box =>
      let length = v.size().u64()
      if length <= I32.max_value().u64() then
        @sqlite3_bind_blob(stmt, bind_index, v.cpointer(), v.size().u32(), addressof this._noop)
      else
        @sqlite3_bind_blob64(stmt, bind_index, v.cpointer(), length, addressof this._noop)
      end
    end
    bind_index = bind_index + 1

// MARK: ---------------------------- CLASS SQLITEQUERYITER ----------------------------

class SqliteQueryIter is Iterator[SqliteRow]
  let stmt: Pointer[Sqlite3Stmt] tag
  var rc:SqliteResultCode = Sqlite.ok()
  
  new create(stmt': Pointer[Sqlite3Stmt] tag) =>
    stmt = stmt'
  
  fun ref finish()? =>
    while has_next() do next()? end
    
  fun ref has_next(): Bool =>
    rc = @sqlite3_step(stmt)
    if rc != Sqlite.row() then
      @sqlite3_finalize(stmt)
      return false
    end
    true

  fun ref next(): SqliteRow? =>
    if rc != Sqlite.row() then
      error
    end
    SqliteRow(stmt)

// MARK: ---------------------------- CLASS SQLITEROW ----------------------------

class SqliteRow
  let stmt: Pointer[Sqlite3Stmt] tag
  let numCols:U32
  new create(stmt': Pointer[Sqlite3Stmt] tag) =>
    stmt = stmt'
    numCols = @sqlite3_column_count(stmt)
  
  fun string(i:U32):String iso^? =>
    if i >= numCols then error end
    String.from_cstring(@sqlite3_column_text(stmt, i)).clone()
  
  fun i32(i:U32):I32? =>
    if i >= numCols then error end
    @sqlite3_column_int(stmt, i).i32()
  
  fun i64(i:U32):I64? =>
    if i >= numCols then error end
    @sqlite3_column_int64(stmt, i)
  
  fun f64(i:U32):F64? =>
    if i >= numCols then error end
    @sqlite3_column_double(stmt, i)
  
  fun bytes(i:U32):Array[U8]? =>
    if i >= numCols then error end
    let arr = @sqlite3_column_blob(stmt, i)
    let length = @sqlite3_column_bytes(stmt, i)
    Array[U8].from_cpointer(arr.convert[U8](), length.usize()).clone()

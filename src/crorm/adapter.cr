require "sqlite3"

module ::DB::QueryMethods
  def write_one(query : String, *args_, args : Array? = nil, as as_type : Tuple | NamedTuple | Class)
    query_one(query, *args_, args: args, as: as_type)
  end

  def write_all(query : String, *args_, args : Array? = nil, as as_type : Tuple | NamedTuple | Class)
    query_all(query, *args_, args: args, as: as_type)
  end
end

module Crorm::DBX
  alias DB_ = ::DB::Database | ::DB::Connection

  abstract def open_ro(&block : DB_ ->)
  abstract def open_rw(&block : DB_ ->)

  def open_tx(&)
    open_rw do |db|
      db.exec "begin transaction"
      result = yield db
      db.exec "commit"
      result
    rescue ex
      db.exec "rollback"
      raise ex
    end
  end

  ####

  def query_one?(query : String, *args_, args : Array? = nil, as as_type : Tuple | NamedTuple | Class)
    open_ro(&.query_one?(query, *args_, args: args, as: as_type))
  end

  def query_one(query : String, *args_, args : Array? = nil, as as_type : Tuple | NamedTuple | Class)
    open_ro(&.query_one(query, *args_, args: args, as: as_type))
  end

  def query_all(query : String, *args_, args : Array? = nil, as as_type : Tuple | NamedTuple | Class)
    open_ro(&.query_all(query, *args_, args: args, as: as_type))
  end

  def write_one(query : String, *args_, args : Array? = nil, as as_type : Tuple | NamedTuple | Class)
    open_rw(&.write_one(query, *args_, args: args, as: as_type))
  end

  def write_all(query : String, *args_, args : Array? = nil, as as_type : Tuple | NamedTuple | Class)
    open_tx(&.write_all(query, *args_, args: args, as: as_type))
  end

  def exec(query : String, *args_, args : Array? = nil)
    open_rw(&.exec(query, *args_, args: args))
  end

  def exec_all(query : String, delimiter = ";")
    open_tx do |db|
      query.split(delimiter, remove_empty: true) do |sql|
        db.exec(sql) unless sql.blank?
      end
    end
  end
end

class Crorm::SQ3
  include Crorm::DBX

  Log = ::Log.for("crorm_sq3")

  def initialize(@db_path : String,
                 init_sql : String = "")
    exec_all(init_sql) unless File.file?(db_path)
  end

  def open_ro : ::DB::Database
    ::DB.open("sqlite3:#{@db_path}?immutable=1")
  end

  def open_rw : ::DB::Database
    ::DB.open("sqlite3:#{@db_path}?synchronous=1")
  end

  def open_ro(&)
    db = self.open_ro
    yield db ensure db.close
  end

  def open_rw(&)
    db = self.open_rw
    yield db ensure db.close
  end
end

class Crorm::PGX
  include Crorm::DBX

  Log = ::Log.for("crorm_pgx")

  getter db : ::DB::Database

  def initialize(db_url : String)
    @db = ::DB.open(db_url)
    at_exit { @db.close rescue nil }
  end

  def open_ro : ::DB::Database
    # TODO: open connection?
    @db
  end

  def open_rw : ::DB::Database
    # TODO: open connection?
    @db
  end

  def open_ro(&)
    # TODO: open connection?
    yield @db
  end

  def open_rw(&)
    # TODO: open connection?
    yield @db
  end
end

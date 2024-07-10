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
  alias DBS = ::DB::Database | ::DB::Connection

  abstract def open_ro(&block : DBS ->)
  abstract def open_rw(&block : DBS ->)

  def init_db
  end

  def open_tx(&)
    open_rw do |db|
      db.exec "BEGIN IMMEDIATE"
      result = yield db
      db.exec "COMMIT"
      result
    rescue ex
      db.exec "ROLLBACK"
      raise ex
    end
  end

  ####

  def query_one?(query : String, *args_, args : Array? = nil, as as_type : Tuple | NamedTuple | Class)
    open_ro do |db|
      db.query_one?(query, *args_, args: args, as: as_type)
    rescue ex : SQLite3::Exception
      case ex.message || ""
      when .includes?("table")
        self.init_db
        db.query_one?(query, *args_, args: args, as: as_type)
      else
        raise ex
      end
    end
  end

  def query_one(query : String, *args_, args : Array? = nil, as as_type : Tuple | NamedTuple | Class)
    open_ro do |db|
      db.query_one(query, *args_, args: args, as: as_type)
    rescue ex : SQLite3::Exception
      case ex.message || ""
      when .includes?("table")
        self.init_db
        db.query_one(query, *args_, args: args, as: as_type)
      else
        raise ex
      end
    end
  end

  def query_all(query : String, *args_, args : Array? = nil, as as_type : Tuple | NamedTuple | Class)
    open_ro(&.query_all(query, *args_, args: args, as: as_type))
  end

  def write_one(query : String, *args_, args : Array? = nil, as as_type : Tuple | NamedTuple | Class)
    open_rw do |db|
      db.write_one(query, *args_, args: args, as: as_type)
    rescue ex : SQLite3::Exception
      case ex.message || ""
      when .includes?("locked")
        sleep 0.5
        db.write_one(query, *args_, args: args, as: as_type)
      when .includes?("table")
        self.init_db
        db.write_one(query, *args_, args: args, as: as_type)
      else
        raise ex
      end
    end
  end

  def write_all(query : String, *args_, args : Array? = nil, as as_type : Tuple | NamedTuple | Class)
    open_tx do |db|
      db.write_all(query, *args_, args: args, as: as_type)
    rescue ex : SQLite3::Exception
      case ex.message || ""
      when .includes?("locked")
        sleep 0.5
        db.write_all(query, *args_, args: args, as: as_type)
      when .includes?("table")
        self.init_db
        db.write_all(query, *args_, args: args, as: as_type)
      else
        raise ex
      end
    end
  end

  def exec(query : String, *args_, args : Array? = nil)
    open_rw(&.exec(query, *args_, args: args))
  end

  def exec_all(query : String, delimiter = ";")
    open_rw do |db|
      query.split(delimiter, remove_empty: true) do |sql|
        db.exec(sql) unless sql.blank?
      end
    end
  end
end

class Crorm::SQ3
  include Crorm::DBX

  Log = ::Log.for("crorm_sq3")

  getter db_path : String

  def initialize(@db_path, &)
    if stat = File.info?(db_path)
      return if stat.size > 0
      File.delete(db_path) # reinit file if invalid
    end

    yield self
  end

  def initialize(@db_path, init_sql : String = "")
    return if init_sql.empty?

    if stat = File.info?(db_path)
      return if stat.size > 0
      File.delete(db_path)
    end

    self.init_db(init_sql)
  end

  def init_db(sql : String)
    Dir.mkdir_p(File.dirname(@db_path))
    self.exec_all(sql)
  end

  def open_ro : DBS
    ::DB.connect("sqlite3:#{@db_path}?journal_mode=WAL&immutable=1&busy_timeout=5000&cache_size=10000&temp_store=MEMORY")
  end

  def open_rw : DBS
    ::DB.connect("sqlite3:#{@db_path}?journal_mode=WAL&synchronous=1&busy_timeout=5000&cache_size=10000&temp_store=MEMORY")
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

  getter db : DBS

  def self.new(db_url : String, use_pool = false)
    new(use_pool ? ::DB.open(db_url) : ::DB.connect(db_url))
  end

  def initialize(@db)
  end

  def finalize
    @db.close rescue nil
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

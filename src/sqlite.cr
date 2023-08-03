require "sqlite3"

class Crorm::SQ3
  def self.init_db(db_path : String, init_sql : String, reset : Bool = false)
    File.delete?(db_path) if reset

    self.open_tx(db_path) do |db|
      init_sql.split(";", remove_empty: true).each do |sql|
        db.exec(sql) unless sql.blank?
      end
    end
  end

  def self.open_db(db_path : String)
    DB.open("sqlite3:#{db_path}?jornal_mode=WAL&synchronous=normal")
  end

  def self.open_db(db_path : String, &)
    db = open_db(db_path)

    begin
      yield db
    ensure
      db.close
    end
  end

  def self.open_tx(db_path : String)
    open_db(db_path) do |db|
      db.exec "begin"
      yield db
      db.exec "commit"
    rescue ex
      db.exec "rollback"
      raise ex
    end
  end

  ####

  getter db_path : String

  getter db : ::DB::Database do
    @expiry = Time.utc + @ttl

    spawn do
      loop do
        sleep @ttl
        break if @expiry < Time.utc
      end

      self.close_db(@db)
    end

    self.class.open_db(@db_path)
  end

  getter? on_tx : Bool = false

  @expiry = Time.utc

  def initialize(@db_path : String, init_sql : String? = nil, @ttl = 30.seconds)
    init_db(init_sql) unless File.file?(@db_path) || init_sql.nil?
  end

  def init_db(init_sql : String, reset : Bool = false)
    self.class.init_db(db_path, init_sql, reset: reset)
  end

  def close_db(db = @db)
    db.try(&.close)
    @db = nil
  end

  def finalize
    close_db rescue nil
  end

  def open_db(&)
    yield self.db
  end

  def open_tx(&)
    open_db do |db|
      db.exec("begin")
      yield db
      db.exec("commit")
    rescue ex
      db.exec("rollback")
      raise ex
    end
  end

  def begin_tx(db = self.db)
    return if @on_tx
    db.exec("begin")
    @on_tx = true
  end

  def commit_tx(db = self.db)
    db.exec("commit") if @on_tx
    @on_tx = false
  end

  def discard_tx(db = self.db)
    db.exec("rollback") if @on_tx
    @on_tx = false
  end
end

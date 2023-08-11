require "sqlite3"

class Crorm::SQRepo
  Log = ::Log.for("sqlite3_db")

  def initialize(@db_path : String,
                 init_sql : String = "")
    exec_all(init_sql) unless File.file?(db_path)
  end

  def open_ro
    DB.open("sqlite3:#{@db_path}?immutable=1")
  end

  def open_ro(&)
    db = self.open_ro
    yield db ensure db.close
  end

  def open_rw
    DB.open("sqlite3:#{@db_path}?synchronous=1")
  end

  def open_rw(&)
    db = self.open_rw
    yield db ensure db.close
  end

  def open_tx(&)
    open_rw do |db|
      db.exec "begin transaction"
      result = yield db
      db.exec "commit"
      result
    rescue
      db.exec "rollback"
    end
  end

  def exec_all(sql : String, delimiter = ";")
    open_tx do |db|
      sql.split(delimiter, remove_empty: true) { |sql| db.exec(sql) unless sql.blank? }
    end
  end
end

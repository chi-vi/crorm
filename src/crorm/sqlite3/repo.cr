require "./db"

class Crorm::Sqlite3::Repo
  def initialize(@path : String, init_sql : String? = nil)
    init_db(init_sql, reset: false) unless File.file?(@path) || init_sql.nil?
  end

  def init_db(init_sql : String, reset : Bool = false)
    File.delete?(@path) if reset

    open_db do |db|
      init_sql.split(";\n").each { |query| db.exec(query) unless query.blank? }
    end
  end

  def open_db
    db = DB.new(@path)
    yield db
  ensure
    db.close
  end

  def open_tx
    open_db do |db|
      db.start_tx
      yield db
      db.commit_tx
    end
  end

  def insert(table : String,
             fields : Enumerable(String),
             values : Enumerable(DB::Any),
             mode : SQL::InsertMode = :default)
    open_db(&.insert(table, fields, value, mode))
  end

  def upsert(table : String,
             fields : Enumerable(String),
             values : Enumerable(DB::Any),
             update_fields = fields)
    open_db { |db| db.upsert(table, fields, values, update_fields) }
  end

  def upsert(table : String,
             fields : Enumerable(String),
             values : Enumerable(DB::Any))
    open_db { |db| db.upsert(table, fields, values) { |sql| yield sql } }
  end

  def update(table : String,
             fields : Enumerable(String),
             values : Enumerable(DB::Any))
    open_db(&.update(table, fields, values))
  end
end

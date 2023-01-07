require "./sql"

class Crorm::Sqlite3::Repo
  def initialize(@path : String, init_sql : String? = nil)
    init_db(init_sql, reset: false) unless File.file?(@path) || init_sql.nil?
  end

  def init_db(init_sql : String, reset : Bool = false)
    File.delete?(@path) if reset

    open_db do |db|
      init_sql.split(";").each { |query| db.exec(query) }
    end
  end

  def open(&block)
    DB.open("sqlite3:#{@path}") { |db| yield db }
  end

  def open_tx(&block)
    open do |db|
      db.exec "pragma journal_mode = WAL"
      db.exec "pragma synchronous = normal"

      db.exec "begin"
      yield db
      db.exec "commit"
    end
  end

  def insert(table : String, fields : Array(String), values : Array(DB::Any),
             on_conflict : SqlBuilder::ConflictResolution = :fail)
    insert(table, fields, values) { |sql| SqlBuilder.on_conflict(sql, on_conflict) }
  end

  def insert(table : String, fields : Array(String), values : Array(DB::Any))
    open_tx do |db|
      query = SqlBuilder.insert_sql(table, fields) { |sql| yield sql }
      db.exec query, args: values
    end
  end

  def upsert(table : String, fields : Array(String), values : Array(DB::Any),
             update_fields = fields)
    upsert(table, fields, values) do |sql|
      build_upsert_sql(sql, update_fields)
    end
  end

  def upsert(table : String, fields : Array(String), values : Array(DB::Any))
    open_tx do |db|
      query = SqlBuilder.upsert_sql(table, fields) { |sql| yield sql }
      db.exec query, args: values
    end
  end
end

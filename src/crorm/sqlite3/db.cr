require "db"
require "./sql"

class Crorm::Sqlite3::DB
  getter db : ::DB::Database
  forward_missing_to db

  getter? on_tx : Bool = false

  def initialize(path : String)
    @db = ::DB.open("sqlite3://#{path}")
    @db.exec "pragma journal_mode = WAL"
    @db.exec "pragma synchronous = normal"
  end

  def finalize
    @db.close
  end

  @[AlwaysInline]
  def start_tx
    @on_tx = true
    @db.exec "begin"
  end

  @[AlwaysInline]
  def commit_tx
    @db.exec "commit"
    @on_tx = false
  end

  def transact(&block)
    if self.on_tx?
      yield @db
    else
      start_tx
      yield @db
      commit_tx
    end
  end

  def insert(table : String,
             fields : Enumerable(String),
             values : Enumerable(::DB::Any),
             mode : SQL::InsertMode = :default)
    @db.exec SQL.insert_sql(table, fields, mode), args: values
  end

  def upsert(table : String,
             fields : Enumerable(String),
             values : Enumerable(::DB::Any),
             update_fields = fields)
    upsert(table, fields, values) { |sql| SQL.build_upsert_sql(sql, update_fields) }
  end

  def upsert(table : String,
             fields : Enumerable(String),
             values : Enumerable(::DB::Any))
    query = SQL.upsert_sql(table, fields) { |sql| yield sql }
    @db.exec query, args: values
  end

  def update(table : String,
             fields : Enumerable(String),
             value : Enumerable(::DB::Any))
    query = SQL.update_sql(table, fields) { |sql| yield sql }
    @db.exec query, args: values
  end
end

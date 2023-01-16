require "db"
require "sqlite3"

module Crorm::Sqlite3::SQL
  extend self

  QUOTING_CHAR = '"'

  # quotes table and column names
  def quote(name : String) : String
    "#{QUOTING_CHAR}#{name}#{QUOTING_CHAR}"
  end

  # :ditto:
  def quote(io : IO, name : String) : IO
    io << QUOTING_CHAR << name << QUOTING_CHAR
  end

  def insert_sql(table : String, fields : Array(String))
    raise "fields are empty!" if fields.empty?

    String.build do |sql|
      build_insert_sql(sql, table, fields)
      yield sql
    end
  end

  enum InsertMode
    Default; Replace; Ignore

    def to_sql
      return "insert into " if self.default?
      "insert or #{self.to_s.downcase} into "
    end
  end

  def insert_sql(table : String, fields : Array(String), mode : InsertMode = :default)
    String.build { |sql| build_insert_sql(sql, table, fields, mode) }
  end

  def upsert_sql(table : String, fields : Array(String), conflict_fields : String = "")
    upsert_sql(table, fields, conflict_fields) do |sql|
      build_upsert_sql(sql, fields)
    end
  end

  def upsert_sql(table : String, fields : Array(String), conflict_fields : String = "", &block)
    raise "fields are empty!" if fields.empty?

    String.build do |sql|
      build_insert_sql(sql, table, fields)

      sql << " on conflict"
      sql << '(' << conflict_fields << ')' unless conflict_fields.empty?
      sql << " do update set "
      yield sql
    end
  end

  private def build_insert_sql(sql : IO, table : String, fields : Enumerable(String), mode : InsertMode = :default)
    sql << mode.to_sql << quote(table) << '('
    fields.join(sql, ", ") { |s, i| quote(i, s) }
    sql << ") values ("
    fields.join(sql, ", ") { |_, io| io << '?' }
    sql << ")"
  end

  def build_upsert_sql(sql : IO, fields : Enumerable(String))
    fields.join(sql, ", ") do |field, io|
      quote(io, field) << " = excluded." << field
    end
  end

  def update_sql(table : String, fields : Enumerable(String), conds : Enumerable(String))
    update_sql(table, fields) do |sql|
      sql << " where "
      conds.join(sql, " and ") { |cond, io| io << '(' << cond << ')' }
    end
  end

  def update_sql(table : String, fields : Enumerable(String), &)
    String.build do |sql|
      sql << "update table " << quote(table) << "set "
      fields.join(sql, ", ") { |f, io| io << quote(f) << " = ?" }
      yield sql
    end
  end
end

require "db"

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

  private def build_insert_sql(sql : IO, table : String, fields : Array(String), mode : InsertMode = :default)
    sql << mode.to_sql << quote(table) << '('
    fields.join(sql, ", ") { |s, i| quote(i, s) }
    sql << ") values ("
    fields.join(sql, ", ") { |_, io| io << '?' }
    sql << ")"
  end

  def build_upsert_sql(sql : IO, fields : Array(String))
    fields.join(sql, ", ") do |field, io|
      quote(io, field) << " = excluded." << field
    end
  end
end

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

  enum ConflictResolution
    Ignore; Replace; Abort; Rollback; Upsert
  end

  def insert_sql(table : String, fields : Array(String),
                 on_conflicts : Hash(String, ConflictResolution | Array(String)))
    insert_sql(table, field) do |sql|
      conflicts.each do |conflict_fields, resolution|
        resolution = fields if resolution.is_a?(ConflictResolution) && resolution.upsert?
        on_conflict(sql, resolution, conflict_fields)
      end
    end
  end

  def on_conflict(sql : IO, resolution : ConflictResolution = :ignore, conflict_fields : String = "")
    sql << " on conflict"
    sql << '(' << conflict_fields << ')' unless conflict_fields.empty?
    sql << ' ' << resolution
  end

  def on_conflict(sql : IO, update_fields : Array(String), conflict_fields : String = "")
    sql << " on conflict"
    sql << '(' << conflict_fields << ')' unless conflict_fields.empty?
    sql << " do update set "
    build_upsert_sql(sql, update_fields)
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

  def build_insert_sql(sql : IO, table : String, fields : Array(String))
    sql << "insert into " << table << '('
    fields.join(io, ", ") { |s, i| quote(i, s) }
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

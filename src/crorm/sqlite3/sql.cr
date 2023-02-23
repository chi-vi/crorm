module Crorm::Sqlite3::SQL
  extend self

  QUOTE = '"'

  # quotes table and column names
  def quote(name : String) : String
    "#{QUOTE}#{name}#{QUOTE}"
  end

  # :ditto:
  def quote(io : IO, name : String) : IO
    io << QUOTE << name << QUOTE
  end

  def insert_smt(table : String, fields : Enumerable(String), mode = "insert")
    insert_smt(table, fields, mode) { }
  end

  def insert_smt(table : String, fields : Enumerable(String), mode = "insert", &)
    String.build do |smt|
      build_insert_clause(smt, table, fields, mode)
      yield smt
    end
  end

  def upsert_smt(table : String, fields : Enumerable(String), on_conflict : String? = nil, skip_fields : Enumerable(String)? = nil, where_clause : String? = nil)
    String.build do |smt|
      build_insert_clause(smt, table, fields)

      smt << " on conflict "
      smt << on_conflict if on_conflict
      smt << " do update set "

      fields = fields.reject(&.in?(skip_fields)) if skip_fields
      build_update_clause(smt, fields, where_clause: where_clause)
    end
  end

  def update_smt(table : String, fields : Enumerable(String), clauses : Enumerable(String))
    update_smt(table, fields, where_clause: build_where_clause(clauses))
  end

  def update_smt(table : String, fields : Enumerable(String), where_clause : String? = nil)
    String.build do |smt|
      smt << "update table " << quote(table) << "set "
      fields.join(smt, ", ") { |f, io| quote(io, f) << " = ?" }
      smt << "where #{where_clause}" if where_clause
    end
  end

  def build_where_clause(clauses : Enumerable(String))
    String.build { |smt| build_where_clause(smt, clauses) }
  end

  def build_where_clause(smt : IO, clauses : Enumerable(String))
    clauses.join(smt, " and ") { |clause, io| io << '(' << clause << ')' }
  end

  private def build_insert_clause(smt : IO, table : String, fields : Enumerable(String), mode = "insert")
    smt << mode << " into " << quote(table) << '('
    fields.join(smt, ", ") { |s, i| quote(i, s) }
    smt << ") values ("
    fields.join(smt, ", ") { |_, io| io << '?' }
    smt << ')'
  end

  private def build_update_clause(smt : IO, fields : Enumerable(String), where_clause : String? = nil)
    fields.join(smt, ", ") do |field, io|
      quote(io, field)
      io << " = excluded."
      quote(io, field)
    end

    smt << "where " << where_clause if where_clause
  end

  def delete_smt(table : String, where_clause : String? = nil)
    String.build do |smt|
      smt << "delete from "
      quote(stm, table)
      smt << "where " << where_clause if where_clause
    end
  end
end

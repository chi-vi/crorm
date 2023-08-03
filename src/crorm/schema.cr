class Crorm::Schema
  enum Dialect
    Postgres
    Sqlite

    def quote_char
      case self
      in Sqlite   then '"'
      in Postgres then '"'
      end
    end
  end

  getter table : String

  getter db_fields = [] of String
  getter pk_fields = [] of String
  getter upsert_fields = [] of String

  @quote_char = '"'

  def initialize(@table, @dialect : Dialect = :sqlite)
    @quote_char = @dialect.quote_char
  end

  private def quote(io : IO, field_count : Int32)
    field_count.times do |idx|
      io << ", " if idx > 0
      io << '$' << (idx &+ 1)
    end
  end

  private def quote(io : IO, field : String)
    io << @quote_char << field << @quote_char
  end

  private def quote(io : IO, fields : Enumerable(String))
    fields.join(io, ", ") { |field, _| quote(io, field) }
  end

  private def quote(io : IO, fields : Enumerable(String), &)
    fields.join(io, ", ") do |field, _|
      quote(io, field)
      yield field
    end
  end

  private def build_insert_stmt(stmt : IO, mode = "insert")
    stmt << mode << " into " << @quote_char << @table << @quote_char << '('
    quote(stmt, @db_fields)
    stmt << ") values ("
    quote(stmt, @db_fields.size)
    stmt << ')'
  end

  private def add_returning_stmt(stmt : IO)
    stmt << " returning "
    quote(stmt, @db_fields)
  end

  def select_stmt(fields = @db_fields, &)
    String.build do |stmt|
      stmt << "select "
      quote(stmt, fields)
      stmt << " from "
      quote(stmt, @table)

      yield stmt
    end
  end

  def select_stmt(fields = @db_fields)
    select_stmt(fields) { }
  end

  def select_by_id(fields = @db_fields)
    select_stmt(fields, &.<< "where id = $1")
  end

  def insert_stmt
    insert_stmt { |stmt| }
  end

  def insert_stmt(&)
    String.build do |stmt|
      build_insert_stmt(stmt)

      yield stmt

      add_returning_stmt(stmt)
    end
  end

  def upsert_stmt(conflicts = @pk_fields, keep_fields = @upsert_fields)
    upsert_stmt(conflicts, keep_fields) { |stmt| }
  end

  def upsert_stmt(conflicts = @pk_fields, keep_fields = @upsert_fields, &)
    String.build do |stmt|
      build_insert_stmt(stmt)

      stmt << " on conflict ("
      quote(stmt, conflicts)
      stmt << ") do update set "

      quote(stmt, keep_fields) do |field|
        stmt << " = excluded."
        quote(stmt, field)
      end

      yield stmt

      add_returning_stmt(stmt)
    end
  end

  def update_stmt(fields : Enumerable(String) = @upsert_fields)
    update_stmt(fields) do |stmt|
      where_pk_stmt(stmt, index: fields.size &+ 1)
    end
  end

  def update_stmt(fields : Enumerable(String) = @upsert_fields, &)
    String.build do |stmt|
      stmt << "update table "
      quote(stmt, @table)
      stmt << " set "

      fields.each_with_index(1) do |field, idx|
        stmt << ", " if idx > 1
        quote(stmt, field) << " = $" << idx
      end

      yield stmt

      add_returning_stmt(stmt)
    end
  end

  def where_pk_stmt(stmt : IO, fields = @pk_fields, index = 1)
    stmt << " where "

    fields.join(stmt, " and ") do |field, _|
      stmt << '('
      quote(stmt, field)
      stmt << " = $" << index << ')'
      index &+= 1
    end
  end

  def where_db_stmt(stmt : IO, clauses : Enumerable(String), index = 1)
    stmt << " where "

    clauses.join(stmt, " and ") do |clause, _|
      stmt << '('

      if clause.includes?('?')
        stmt << clause.sub("?", "$#{index}")
        index &+= 1
      else
        stmt << clause
      end

      stmt << ')'
    end
  end

  def delete_stmt(fields = @pk_fields)
    delete_stmt do |stmt|
      where_pk_stmt(stmt, fields)
    end
  end

  def delete_stmt(&)
    String.build do |stmt|
      stmt << "delete from "
      quote(stmt, table)
      yield stmt
    end
  end
end

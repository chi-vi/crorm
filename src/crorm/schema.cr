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

  getter insert_fields = [] of String
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
    stmt << mode << " into " << @quote_char << @table << @quote_char << "("
    quote(stmt, @insert_fields)
    stmt << ") values ("
    quote(stmt, @insert_fields.size)
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

  def select_stmt(fields : Enumerable(String) = @db_fields)
    select_stmt(fields) { }
  end

  def select_by_pkey(fields : Enumerable(String) = @db_fields)
    select_stmt(fields) do |sql|
      sql << " where 1=1"

      @pk_fields.each_with_index(1) do |field, index|
        sql << " and ("
        quote(sql, field)
        sql << " = $" << index << ')'
      end
    end
  end

  def insert_stmt
    insert_stmt { }
  end

  def insert_stmt(&)
    String.build do |stmt|
      build_insert_stmt(stmt)
      yield stmt
      add_returning_stmt(stmt)
    end
  end

  def upsert_stmt(conflicts : Enumerable(String) = @pk_fields, keep_fields : Enumerable(String) = @upsert_fields)
    upsert_stmt(conflicts, keep_fields) { }
  end

  def upsert_stmt(conflicts : Enumerable(String) = @pk_fields, keep_fields : Enumerable(String) = @upsert_fields, &)
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
    update_stmt(fields) { |stmt| where_pk_stmt(stmt, index: fields.size &+ 1) }
  end

  def update_stmt(fields : Enumerable(String) = @upsert_fields, &)
    String.build do |stmt|
      stmt << "update "
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
    stmt << " where 1 = 1"

    fields.each do |field|
      stmt << " and ("
      quote(stmt, field)
      stmt << " = $" << index << ')'
      index &+= 1
    end
  end

  def where_db_stmt(stmt : IO, clauses : Enumerable(String), index = 1)
    stmt << " where 1 = 1"

    clauses.each do |clause|
      stmt << " and ("

      if clause.includes?('?')
        stmt << clause.sub("?", "$#{index}") << ')'
        index &+= 1
      else
        stmt << clause << ')'
      end
    end
  end

  def delete_stmt(fields = @pk_fields)
    delete_stmt { |stmt| where_pk_stmt(stmt, fields) }
  end

  def delete_stmt(&)
    String.build do |stmt|
      stmt << "delete from "
      quote(stmt, table)
      yield stmt
    end
  end
end

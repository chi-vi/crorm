require "db"
require "json"

module Crorm::Model
  macro included
    include ::DB::Serializable
    include ::DB::Serializable::NonStrict

    include ::JSON::Serializable
  end

  @@db_fields = [] of String
  @@pk_fields = [] of String

  def pk_values
    {% begin %}
      {
        {% for field in @type.instance_vars %}
          {% ann = field.annotation(DB::Field) %}
          {% if ann && ann[:primary] %}
            { {{field.name.stringify}}, @{{ field.name.id }} },
          {% end %}
        {% end %}
      }
    {% end %}
  end

  def db_values
    {% begin %}
      {% fields = @type.instance_vars.select(&.annotation(DB::Field)) %}
      {
        {% for field in fields %}
          {% if field.type.has_method?(:to_db) %}
            @{{field.name.id}}.to_db,
          {% else %}
            @{{field.name.id}},
          {% end %}
        {% end %}
      }
    {% end %}
  end

  def db_changes
    fields = [] of String
    values = [] of DB::Any

    {% for field in @type.instance_vars.select(&.annotation(DB::Field)) %}
      {% ann = field.annotation(DB::Field) %}

      {% if !ann[:ignore] %}
        field = {{ field.name.stringify }}
        value = @{{ field.name.id }}

        {% begin %}
          if value || {{ ann[:nilable].id }}
            fields << field
            {% if field.type.has_method?(:to_db) %}
            values << value.to_db
            {% else %}
            values << value
            {% end %}
          {% if !ann[:presence] %}
          else
            raise "#{field} can not be nil!"
          {% end %}
          end
        {% end %}
      {% end %}
    {% end %}

    {fields, values}
  end

  def db_changes(skip_fields : Enumerable(String))
    fields, values = self.db_changes

    (fields.size - 1).downto(0) do |i|
      next unless skip_fields.includes?(fields.unsafe_fetch(i))

      fields.delete_at(i)
      values.delete_at(i)
    end

    {fields, values}
  end

  def create!(repo = self.class.repo, mode = "insert")
    fields, values = self.db_changes
    repo.insert(@@table, fields, values)
  end

  def update!(repo = self.class.repo)
    where_clause = String.build do |io|
      @@pk_fields.join(io, " and ") do |field, _|
        index = @@db_fields.find!(field) &+ 1
        io << "#{field} = $#{index}"
      end
    end

    repo.update(@@table, @@db_fields, @@db_values, where_clause: where_clause)
  end

  def update!(pk_fields : Enumerable(String), pk_values : Enumerable(String), repo = self.class.repo)
    fields, values = self.db_changes(pk_fields)
    values.concat(pk_values)

    where_clause = Crorm::Sqlite3::SQL.build_where_clause(pk_fields)
    repo.update(@@table, fields, values, where_clause: where_clause)
  end

  def upsert!(db = @@db, conflicts = @@pk_fields) : Int32
    stmt = String.build do |io|
      fields = @@db_fields

      io << "insert into #{@@table} ("
      fields.join(io, ", ")

      io << ") values ("
      (1..fields.size).join(io, ", ") { |id, _| io << '$' << id }

      io << ") on conflict ("
      conflicts.join(io, ", ")
      io << ") do update set "

      fields.reject(&.in?(conflicts)).join(io, ", ") do |field|
        io << field << " = excluded." << field
      end

      io << " returning id"
    end

    # Log.debug { stmt.colorize.blue }
    db.query_one(stmt, *self.db_values, as: Int32)
  end

  # Defines a field *decl* with the given *options*.
  macro field(decl, db_key = nil, converter = nil, primary = false, auto = true, virtual = false)
    {% var = decl.var %}
    {% type = decl.type %}
    {% value = decl.value %}
    {% nilable = type.resolve.nilable? %}
    {% autogen = (primary || auto) && value.is_a?(Nop) %}

    @@db_fields << {{(db_key || var).stringify}}
    {% if primary %}@@pk_fields << {{(db_key || var).stringify}}{% end %}

    {% if type.resolve.union? && !nilable %}
      {% raise "The column #{@type.name}##{decl.var} cannot consist of a Union with a type other than `Nil`." %}
    {% end %}

    {% bare_type = nilable ? type.types.reject(&.resolve.nilable?).first : type %}

    @[::DB::Field(
      key: {{db_key || var}},
      converter: {{converter}},
      ignore: {{virtual}},
      nilable: {{nilable}},
      presence: {{primary || auto}},
      primary: {{primary}}
    )]
    {% if autogen %}
      @{{var.id}} : {{bare_type.id}}?
    {% else %}
      @{{var.id}} : {{type.id}} {% unless value.is_a? Nop %} = {{value}} {% end %}
    {% end %}

    {% if autogen || nilable %}
      def {{var.id}}=(value : {{bare_type.id}}?)
        @{{var.id}} = value
      end

      def {{var.id}} : {{bare_type.id}}?
        @{{var}}
      end

      def {{var.id}}! : {{bare_type.id}}
        @{{var}}.not_nil!
      end
    {% else %}
      def {{var.id}}=(value : {{type.id}})
        @{{var.id}} = value
      end

      def {{var.id}} : {{type.id}}
        @{{var}}
      end
    {% end %}
  end

  # include created_at and updated_at that will automatically be updated
  macro timestamps
    field created_at : Time = Time.utc
    field updated_at : Time = Time.utc
  end
end

require "db"
require "json"

module Crorm::Model
  macro included
    include ::DB::Serializable
    include ::JSON::Serializable

    class_getter table : String = self.name.underscore.gsub("::", "_")

    def self.from_rs(rs : DB::ResultSet)
      new.tap(&.from_rs(rs))
    end

    def initialize
    end

    def initialize(&block)
      with self yield
    end

    def initialize(rs : DB::ResultSet)
      from_rs(rs)
    end

    def initialize(tuple : NamedTuple)
      {% verbatim do %}
        {% for field in @type.instance_vars.select(&.annotation(::DB::Field)) %}
          if value = tuple[:{{field.name.stringify}}]?
            @{{field.name.id}} = value
          end
        {% end %}
      {% end %}
    end
  end

  # Consumes the result set to set self's property values.
  def from_rs(rs : DB::ResultSet) : Nil
    rs.column_count.times { |index| from_rs(rs, index) }
  end

  def from_rs(rs : DB::ResultSet, index : Int32)
    {% begin %}
      case rs.column_name(index)
      {% for field in @type.instance_vars.select(&.annotation(DB::Field)) %}
        {% ann = field.annotation(DB::Field) %}
        {% if ann[:ignore] != true %}
        when {{ann[:key].stringify}}
          {{ field_type = ann[:nilable] ? field.type : field.type.union_types.reject(&.nilable?).first }}
          value = {{field_type.id}}.from_rs(rs)

          {% if field.has_default_value? %}
            @{{field.id}} = value unless value.nil?
          {% else %}
            @{{field.id}} = value
          {% end %}
        {% end %}
      {% end %}
      end
    {% end %}
  end

  def pk_field
    {% begin %}
      {% fields = @type.instance_vars.select(&.annotation(DB::Field).try(&.[:primary])) %}

      {% if pk_field = fields[0] %}
        {{ pk_field.name.stringify }}
      {% else %}
        raise "no primary key declared!"
      {% end %}
    {% end %}
  end

  def pk_value
    {% begin %}
      {% fields = @type.instance_vars.select(&.annotation(DB::Field).try(&.[:primary])) %}

      {% if pk_field = fields[0] %}
        @{{ pk_field.name.id }}
      {% else %}
        raise "missing primary key!"
      {% end %}
    {% end %}
  end

  # All database fields
  def db_fields : Array(String)
    fields = [] of String

    {% for field in @type.instance_vars.select(&.annotation(DB::Field)) %}
      {% ann = field.annotation(DB::Field) %}
      {% if !ann[:ignore] %}fields << {{field.name.stringify}}{% end %}
    {% end %}

    fields
  end

  def db_values
    values = [] of DB::Any

    {% for field in @type.instance_vars.select(&.annotation(DB::Field)) %}
      {% ann = field.annotation(DB::Field) %}
      {% if !ann[:ignore] %}values << @{{ field.name.id }}.to_db {% end %}
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
            values << value.to_db
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

  def create!(repo : Crorm::Sqlite3::Repo = self.class.repo, mode = "insert")
    fields, values = self.db_changes
    repo.insert(@@table, fields, values)
  end

  def update!(repo : Crorm::Sqlite3::Repo = self.class.repo)
    pk_field = self.pk_field
    fields, values = self.db_changes(pk_field)
    where_clause = "#{pk_field} = #{self.pk_value}"
    repo.update(@@table, fields, values, where_clause: where_clause)
  end

  def update!(pk_fields : Enumerable(String), pk_values : Enumerable(String), repo : Crorm::Sqlite3::Repo = self.class.repo)
    fields, values = self.db_changes(pk_fields)
    values.concat(pk_values)

    where_clause = Crorm::Sqlite3::SQL.build_where_clause(pk_fields)
    repo.update(@@table, fields, values, where_clause: where_clause)
  end

  # Defines a field *decl* with the given *options*.
  macro field(decl, db_key = nil, converter = nil, primary = false, presence = false, virtual = false)
    {% var = decl.var %}
    {% type = decl.type %}
    {% value = decl.value %}

    {% nilable = type.resolve.nilable? %}

    {% if type.resolve.union? && !nilable %}
      {% raise "The column #{@type.name}##{decl.var} cannot consist of a Union with a type other than `Nil`." %}
    {% end %}

    {% bare_type = nilable ? type.types.reject(&.resolve.nilable?).first : type %}

    @[::DB::Field(
      key: {{db_key || var}},
      converter: {{converter}},
      ignore: {{virtual}},
      nilable: {{nilable}},
      presence: {{presence || primary}}
    )]
    @{{var.id}} : {{bare_type.id}}? {% unless value.is_a? Nop %} = {{value}} {% end %}

    {% if nilable || primary %}
      def {{decl.var.id}}=(value : {{bare_type.id}}?)
        @{{decl.var.id}} = value
      end

      def {{decl.var.id}} : {{bare_type.id}}?
        @{{decl.var}}
      end

      def {{decl.var.id}}! : {{bare_type.id}}
        @{{decl.var}}.not_nil!
      end
    {% else %}
      def {{decl.var.id}}=(value : {{type.id}})
        @{{decl.var.id}} = value
      end

      def {{decl.var.id}} : {{type.id}}
        @{{decl.var}}.not_nil!
      end
    {% end %}
  end

  # include created_at and updated_at that will automatically be updated
  macro timestamps
    field created_at : Time = Time.utc
    field updated_at : Time = Time.utc
  end
end

require "db"
require "json"
require "./sqlite3/converters"

module Crorm::Model
  macro included
    include ::DB::Serializable
    include ::JSON::Serializable

    class_property table : String { self.name.underscore.gsub("::", ".") }

    def initialize
    end

    def initialize(&block)
      with self yield
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

    def initialize(rs : DB::ResultSet)
      from_rs(rs)
    end

    def self.from_rs(rs : DB::ResultSet)
      new.tap(&.from_rs(rs))
    end
  end

  # Consumes the result set to set self's property values.
  def from_rs(rs : DB::ResultSet) : Nil
    rs.column_count.times { |index| from_rs(rs, index) }
  end

  def from_rs(result : DB::ResultSet, index : Int32)
    {% begin %}
      case rs.column_name(index)
      {% for field in @type.instance_vars.select(&.annotation(DB::Field)) %}
        {% ann = field.annotation(DB::Field) %}
        when {{ann[:key].stringify}}
          {% if converter = ann[:converter] %}
            @{{field.id}} = {{converter}}.from_rs(result)
          {% else %}
            {{ field_type = ann[:nilable] ? field.type : field.type.types.reject(&.resolve.nilable?).first }}
            value = DB::Any.from_rs(result, {{field_type.id}})

            {% if field.has_default_value? %}
              @{{field.id}} = value unless value.nil?
            {% else %}
              @{{field.id}} = value
            {% end %}
          {% end %}
      {% end %}
    {% end %}
  end

  # All database fields
  def fields : Array(String)
    {% begin %}
      {% fields = @type.instance_vars.select(&.annotation(::DB::Field)) %}
      {{ fields.map(&.name.stringify) }}
    {% end %}
  end

  def get_changes
    fields = [] of String
    values = [] of DB::Any

    {% for field in @type.instance_vars.select(&.annotation(DB::Field)) %}
      {% ann = field.annotation(DB::Field) %}

      {% if !ann[:ignore] %}
        value = @{{field.name.id}}
        {% if converter = ann[:converter] %}value = {{converter}}.to_db(value) if value{% end %}

        {% begin %}
          {% if ann[:presence] %}if value{% end %}
            fields << {{ field.name.stringify }}
            values << value
          {% if ann[:presence] %}end{% end %}
        {% end %}
      {% end %}
    {% end %}

    {fields, values}
  end

  def get_changes(*ignores : String)
    fields, values = self.get_changes

    (fields.size - 1).downto(0) do |i|
      next unless ignores.includes?(fields.unsafe_fetch(i))

      fields.delete_at(i)
      values.delete_at(i)
    end

    {fields, values}
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
  macro timestamps(converter = nil)
    field created_at : Time = Time.utc, converter: converter
    field updated_at : Time = Time.utc, converter: converter
  end
end

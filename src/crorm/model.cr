require "db"
require "json"

module Crorm
  annotation Column; end
end

module Crorm::Model
  macro included
    include ::DB::Serializable
    include ::JSON::Serializable

    class_property table : String = self.name.underscore.gsub("::", ".")

    def initialize
    end

    def initialize(&block)
      with self yield
    end

    def initialize(tuple : NamedTuple)
      {% verbatim do %}
        {% for column in @type.instance_vars.select(&.annotation(::Crorm::Column)) %}
          if value = tuple[:{{column.name.stringify}}]?
            @{{column.name.id}} = value
          end
        {% end %}
      {% end %}
    end
  end

  # All database fields
  def fields : Array(String)
    {% begin %}
      {% columns = @type.instance_vars.select(&.annotation(::Crorm::Column)) %}
      {{ columns.empty? ? [] of String : columns.map(&.name.stringify) }}
    {% end %}
  end

  def get_changes
    fields = [] of String
    values = [] of DB::Any

    {% for column in @type.instance_vars.select(&.annotation(Crorm::Column)) %}
      {% ann = column.annotation(Crorm::Column) %}

      fields << {{ column.name.stringify }}

      {% if converter = ann[:converter] %}
        values << {{ann[:converter]}}.to_db({{column.name.id}})
      {% else %}
        values << self.{{column.name.id}}
      {% end %}
    {% end %}

    {fields, values}
  end

  def self.from_rs(result : DB::ResultSet)
    new.tap(&.from_rs)
  end

  # Consumes the result set to set self's property values.
  def from_rs(result : DB::ResultSet) : Nil
    {% begin %}
      result.column_names.each do |col|
        case col
        {% for column in @type.instance_vars.select(&.annotation(Crorm::Column)) %}
          {% ann = column.annotation(Crorm::Column) %}
          when {{column.name.stringify}}
            @{{column.id}} = {% if converter = ann[:converter] %}
              {{converter}}.from_rs(result)
            {% else %}
              value = DB::Any.from_rs(result, {{ann[:nilable] ? column.type : column.type.union_types.reject(&.== Nil).first}})

              {% if column.has_default_value? && !column.default_value.nil? %}
                return {{column.default_value}} if value.nil?
              {% end %}

              value
            {% end %}
        {% end %}
        else
          @@_remains[col] = DB::Any.from_rs(result)
        end
      end
    {% end %}
  end

  # Defines a column *decl* with the given *options*.
  macro column(decl, column_type = nil, converter = nil, auto = false, primary = false)
    {% type = decl.type %}
    {% nilable = type.resolve.nilable? %}

    # Raise an exception if the delc type has more than 2 union types or if it has 2 types without nil
    # This prevents having a column typed to String | Int32 etc.
    {% if type.resolve.union? && !nilable %}
      {% raise "The column #{@type.name}##{decl.var} cannot consist of a Union with a type other than `Nil`." %}
    {% end %}


    @[::Crorm::Column(column_type: {{column_type}}, converter: {{converter}}, auto: {{auto || primary}}, primary: {{primary}}, nilable: {{nilable}})]
    @{{decl.var}} : {{decl.type}}? {% unless decl.value.is_a? Nop %} = {{decl.value}} {% end %}

    {% if nilable || primary %}
      {% bare_type = nilable ? type.types.reject(&.nilable?).first : type %}
      def {{decl.var.id}}=(value : {{bare_type}}?)
        @{{decl.var.id}} = value
      end

      def {{decl.var.id}} : {{bare_type}}?
        @{{decl.var}}
      end

      def {{decl.var.id}}! : {{bare_type}}
        raise NilAssertionError.new {{@type.name.stringify}} + "#" + {{decl.var.stringify}} + " cannot be nil" if @{{decl.var}}.nil?
        @{{decl.var}}.not_nil!
      end
    {% else %}
      def {{decl.var.id}}=(value : {{type.id}})
        @{{decl.var.id}} = value
      end

      def {{decl.var.id}} : {{type.id}}
        raise NilAssertionError.new {{@type.name.stringify}} + "#" + {{decl.var.stringify}} + " cannot be nil" if @{{decl.var}}.nil?
        @{{decl.var}}.not_nil!
      end
    {% end %}
  end

  # include created_at and updated_at that will automatically be updated
  macro timestamps
    column created_at : Time?
    column updated_at : Time?
  end
end

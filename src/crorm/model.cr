require "db"
require "json"

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
        {% for field in @type.instance_vars.select(&.annotation(::DB::Field)) %}
          if value = tuple[:{{field.name.stringify}}]?
            @{{field.name.id}} = value
          end
        {% end %}
      {% end %}
    end
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
        fields << {{ field.name.stringify }}

        {% if converter = ann[:converter] %}
          values << {{converter}}.to_db({{field.name.id}})
        {% else %}
          values << self.{{field.name.id}}
        {% end %}
      {% end %}
    {% end %}

    {fields, values}
  end

  def get_changes(ignores : Array(String))
    fields, values = self.get_changes

    (fields.size - 1).downto(0) do |i|
      next unless fields.unsafe_fetch(i).in?(ignores)
      fields.delete_at(i)
      values.delete_at(i)
    end

    {fields, values}
  end

  # Defines a field *decl* with the given *options*.
  macro field(decl, db_key = nil, converter = nil, primary = false, virtual = false)
    {% var = decl.var %}
    {% type = decl.type %}
    {% value = decl.value %}

    {% if !converter && type.in?(Time, Enum) %}
      {% converter = type %}
    {% end %}

    @[::DB::Field(key: {{db_key}}, converter: {{converter}}, ignore: {{virtual}}) ]
    @{{var}} : {{type}} {% unless value.is_a? Nop %} = {{value}} {% end %}

    def {{var.id}}=(value : {{type.id}})
      @{{var.id}} = value
    end

    def {{var.id}} : {{type.id}}
      @{{var}}
    end
  end

  # include created_at and updated_at that will automatically be updated
  macro timestamps
    field created_at : Time = Time.utc, converter: Time
    field updated_at : Time = Time.utc, converter: Time
  end
end

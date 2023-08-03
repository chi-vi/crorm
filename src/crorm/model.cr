require "db"
require "json"

require "./schema"

module Crorm::Model
  macro included
    include ::DB::Serializable
    include ::DB::Serializable::NonStrict

    include ::JSON::Serializable

    @[DB::Field(ignore: true)]
    @[JSON::Field(ignore: true)]
    @_fields_changed_ = Set(String).new

    def initialize
    end
  end

  macro schema(table, dialect = :sqlite)
    class_getter schema = ::Crorm::Schema.new({{table}}, :{{dialect.id}})
  end

  # Defines a field *decl* with the given *options*.
  macro field(decl, key = nil, converter = nil, pkey = false, auto = false, ignore = false)
    {% var = decl.var %}
    {% type = decl.type %}
    {% value = decl.value %}
    {% nilable = type.resolve.nilable? %}
    {% autogen = (pkey || auto) && value.is_a?(Nop) %}

    {% if pkey %}@@schema.pk_fields << {{(key || var).stringify}}{% end %}
    {% if !ignore %}@@schema.db_fields << {{(key || var).stringify}}{% end %}
    {% if !auto %}@@schema.upsert_fields << {{(key || var).stringify}}{% end %}

    {% if type.resolve.union? && !nilable %}
      {% raise "The column #{@type.name}##{decl.var} cannot consist of a Union with a type other than `Nil`." %}
    {% end %}

    {% bare_type = nilable ? type.types.reject(&.resolve.nilable?).first : type %}

    @[::DB::Field( key: {{key || var}}, converter: {{converter}}, nilable: {{nilable}}, ignore: {{ignore}}, pkey: {{pkey}}, auto: {{auto}}, )]
    {% if autogen %}
      @{{var.id}} : {{bare_type.id}}?
    {% else %}
      @{{var.id}} : {{type.id}} {% unless value.is_a? Nop %} = {{value}} {% end %}
    {% end %}

    {% if autogen || nilable %}
      def {{var.id}}=(value : {{bare_type.id}}?)
        @_fields_changed_ << {{var.stringify}}
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
        @_fields_changed_ << {{var.stringify}}
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

  def mark_as_changed!(field : String)
    @_fields_changed_ << field
  end

  def pk_values
    {% begin %}
      {
        {% for field in @type.instance_vars %}
          {% ann = field.annotation(DB::Field) %}
          {% if ann && ann[:pkey] %}
            {% if converter = ann[:converter] %}
              {{converter.id}}.to_db(@{{field.name.id}}),
            {% elsif field.type.has_method?(:to_db) %}
              @{{field.name.id}}.to_db,
            {% else %}
              @{{field.name.id}},
            {% end %}
          {% end %}
        {% end %}
      }
    {% end %}
  end

  def db_values
    {% begin %}
      {
        {% for field in @type.instance_vars %}
          {% ann = field.annotation(DB::Field) %}
          {% if ann && !ann[:ignore] %}
            {% if converter = ann[:converter] %}
              {{converter.id}}.to_db(@{{field.name.id}}),
            {% elsif field.type.has_method?(:to_db) %}
              @{{field.name.id}}.to_db,
            {% else %}
              @{{field.name.id}},
            {% end %}
          {% end %}
        {% end %}
      }
    {% end %}
  end

  def update_values
    {% begin %}
      {
        {% for field in @type.instance_vars %}
          {% ann = field.annotation(DB::Field) %}
          {% if ann && (!ann[:ignore] && !ann[:auto]) %}
            {% if converter = ann[:converter] %}
              {{converter.id}}.to_db(@{{field.name.id}}),
            {% elsif field.type.has_method?(:to_db) %}
              @{{field.name.id}}.to_db,
            {% else %}
              @{{field.name.id}},
            {% end %}
          {% end %}
        {% end %}

        {% for field in @type.instance_vars %}
          {% ann = field.annotation(DB::Field) %}
          {% if ann && ann[:pkey] %}
            {% if converter = ann[:converter] %}
              {{converter.id}}.to_db(@{{field.name.id}}),
            {% elsif field.type.has_method?(:to_db) %}
              @{{field.name.id}}.to_db,
            {% else %}
              @{{field.name.id}},
            {% end %}
          {% end %}
        {% end %}
      }
    {% end %}
  end

  def db_changes
    {% begin %}
      {
        {% for field in @type.instance_vars %}
          {% ann = field.annotation(DB::Field) %}
          {% if ann && !(ann[:ignore] || ann[:auto]) %}
            {% if converter = ann[:converter] %}
              { {{ field.name.stringify }} , {{converter.id}}.to_db(@{{field.name.id}}) },
            {% elsif field.type.has_method?(:to_db) %}
              { {{ field.name.stringify }}, @{{field.name.id}}.to_db },
            {% else %}
              { {{ field.name.stringify }} , @{{field.name.id}} },
            {% end %}
          {% end %}
        {% end %}
      }
    {% end %}
  end

  def db_changes(skip_fields : Enumerable(String))
    self.db_changes.reject!(&.[0].in?(skip_fields))
  end

  def insert!(db = self.class.db,
              stmt = @@schema.insert_stmt,
              values = self.db_values)
    db.query_one(stmt, *values, as: self.class)
  end

  def upsert!(db = self.class.db,
              stmt = @@schema.insert_stmt,
              values = self.db_values)
    db.query_one(stmt, *values, as: self.class)
  end

  def update!(db = self.class.db,
              stmt = @@schema.update_stmt,
              values = self.update_values)
    db.query_one(stmt, *values, as: self.class)
  end
end

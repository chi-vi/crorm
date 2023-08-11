require "db"
require "json"

require "./schema"
require "./adapter"

module Crorm::Model
  alias DB_ = ::DB::Database | ::DB::Connection | ::Crorm::DBX

  macro included
    ###

    def self.get_all(*args_, args : Array? = nil, db : DB_ = self.db, as as_type = self, &)
      query = self.schema.select_stmt { |sql| yield sql }
      db.query_all(query, *args_, args: args, as: as_type)
    end

    def self.get_all(*args_, args : Array? = nil, db : DB_ = self.db, as as_type = self)
      self.get_all(*args_, args: args, db: db, as: as_type) { }
    end

    def self.get_all_by_ids(ids : Enumerable, pkey : String = "id", db : DB_ = self.db, as as_type = self)
      self.get_all(query, args: ids, db: db, as: as_type, &.<< "where #{pkey} = any ($1)")
    end

    def self.get(*args_, args : Array? = nil, db : DB_ = self.db, as as_type = self, &)
      query = self.schema.select_stmt { |sql| yield sql; sql << " limit 1" }
      db.query_one?(query, *args_, args: args, as: as_type)
    end

    def self.find(id, pkey = "id", db : DB_ = self.db) : self | Nil
      get(id, db: db, &.<< "where #{pkey} = $1")
    end

    def self.get!(*args_, args : Array? = nil, db : DB_ = self.db, as as_type = self, &)
      query = self.schema.select_stmt { |sql| yield sql; sql << " limit 1" }
      db.query_one(query, *args_, args: args, as: as_type)
    end

    def self.find!(id, pkey = "id", db : DB_ = self.db) : self | Nil
      get!(id, db: db, &.<< "where #{pkey} = $1")
    end
  end

  macro schema(table, dialect = :sqlite, strict = true, multi = false, json = true)
    include ::DB::Serializable
    {% if !strict %}include ::DB::Serializable::NonStrict{% end %}
    {% if json %}include ::JSON::Serializable{% end %}

    class_getter schema = ::Crorm::Schema.new({{table}}, {{dialect}})

    {% if dialect == :sqlite %}
      {% if multi %}
        def self.db_path
          raise "invalid!"
        end

        def self.db
          raise "invalid!"
        end

        def self.db(*input)
          ::Crorm::SQ3.new(db_path(*input), self.init_sql)
        end
      {% else %}
        class_getter db = ::Crorm::SQ3.new(self.db_path, self.init_sql)
      {% end %}
    {% end %}
  end

  # Defines a field *decl* with the given *options*.
  macro field(decl, key = nil, converter = nil, pkey = false, auto = false)
    {% var = decl.var %}
    {% name = (key || var).stringify %}
    {% type = decl.type %}
    {% value = decl.value %}
    {% nilable = type.resolve.nilable? %}
    {% autogen = auto && value.is_a?(Nop) %}

    @@schema.db_fields << {{name}}
    {% if pkey %}@@schema.pk_fields << {{name}}{% end %}
    {% if !auto %}@@schema.insert_fields << {{name}}{% end %}
    {% if !pkey && !auto %}@@schema.upsert_fields << {{name}}{% end %}

    {% if type.resolve.union? && !nilable %}
      {% raise "The column #{@type.name}##{decl.var} cannot consist of a Union with a type other than `Nil`." %}
    {% end %}

    {% bare_type = nilable ? type.types.reject(&.resolve.nilable?).first : type %}

    @[::DB::Field( key: {{name}}, converter: {{converter}}, nilable: {{nilable}}, pkey: {{pkey}}, auto: {{auto}})]
    {% if autogen || nilable %}
      @{{var.id}} : {{bare_type.id}}? = {% if value.is_a? Nop %}nil{% else %}{{value}}{% end %}

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
      @{{var.id}} : {{type.id}} {% unless value.is_a? Nop %} = {{value}} {% end %}

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
          {% if ann && !ann[:auto] %}
            {% if converter = ann[:converter] %}
              {{converter.id}}.to_db(@{{field.name.id}}),
            {% elsif field.type.has_method?(:to_db) %}
              @{{field.name.id}}.try(&.to_db),
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
          {% if ann && !ann[:pkey] && !ann[:auto] %}
            {% if converter = ann[:converter] %}
              {{converter.id}}.to_db(@{{field.name.id}}),
            {% elsif field.type.has_method?(:to_db) %}
              @{{field.name.id}}.try(&.to_db),
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
              @{{field.name.id}}.try(&.to_db),
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
          {% if ann && !ann[:auto] %}
            {% if converter = ann[:converter] %}
              { {{ field.name.stringify }} , {{converter.id}}.to_db(@{{field.name.id}}) },
            {% elsif field.type.has_method?(:to_db) %}
              { {{ field.name.stringify }}, @{{field.name.id}}.try(&.to_db) },
            {% else %}
              { {{ field.name.stringify }}, @{{field.name.id}} },
            {% end %}
          {% end %}
        {% end %}
      }
    {% end %}
  end

  def db_changes(skip_fields : Enumerable(String))
    self.db_changes.reject!(&.[0].in?(skip_fields))
  end

  def insert!(query : String = @@schema.insert_stmt,
              args_ = self.db_values,
              db : DB_ = self.class.db)
    db.write_one(query, *args_, as: self.class)
  end

  def upsert!(query : String = @@schema.upsert_stmt,
              args_ = self.db_values,
              db : DB_ = self.class.db)
    db.write_one(query, *args_, as: self.class)
  end

  def update!(query : String = @@schema.update_stmt,
              args_ = self.update_values,
              db : DB_ = self.class.db)
    db.write_one(query, *args_, as: self.class)
  end
end

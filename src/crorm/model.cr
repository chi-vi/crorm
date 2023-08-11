require "db"
require "json"

require "./schema"
require "./sq_repo"

module Crorm::Model
  macro included
    def self.get_one?(stmt : String, *values, db = self.db, as as_type = self)
      if db.is_a?(Crorm::SQRepo)
        db.open_ro(&.query_one?(stmt, *values, as: as_type))
      else
        db.query_one?(stmt, *values, as: as_type)
      end
    end

    def self.get_one(stmt : String, *values, db = self.db, as as_type = self)
      if db.is_a?(Crorm::SQRepo)
        db.open_ro(&.query_one(stmt, *values, as: as_type))
      else
        db.query_one(stmt, *values, as: as_type)
      end
    end

    ###

    def self.get_all(stmt : String, *values, db = self.db, as as_type = self)
      if db.is_a?(Crorm::SQRepo)
        db.open_ro(&.query_all(stmt, *values, as: as_type))
      else
        db.query_all(stmt, *values, as: as_type)
      end
    end

    ###

    def self.set_one(stmt : String, *values, db = self.db, as as_type = self)
      if db.is_a?(Crorm::SQRepo)
        db.open_tx(&.query_one(stmt, *values, as: as_type))
      else
        db.query_one(stmt, *values, as: as_type)
      end
    end

    ###

    def self.all(*values, db = self.db)
      stmt = self.schema.select_stmt
      self.get_all(stmt, *values, db: db)
    end

    def self.all(*values, db = self.db, &)
      stmt = self.schema.select_stmt { |sql| yield sql }
      self.get_all(stmt, *values, db: db)
    end

    def self.all(ids : Enumerable, pkey : String = "id", db = self.db)
      stmt = self.schema.select_stmt(&.<< "where #{pkey} = any ($1)")
      self.get_all(stmt, ids, db: db)
    end

    def self.get(*values, db = self.db, &)
      stmt = self.schema.select_stmt { |sql| yield sql; sql << " limit 1" }
      self.get_one?(stmt, *values, db: db)
    end

    def self.find(id, pkey = "id", db = self.db) : self | Nil
      get(id, db: db, &.<< "where #{pkey} = $1")
    end

    def self.get!(*values, db = self.db, &)
      stmt = self.schema.select_stmt { |sql| yield sql; sql << " limit 1" }
      self.get_one(stmt, *values, db: db)
    end

    def self.find!(id, pkey = "id", db = self.db) : self | Nil
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
          ::Crorm::SQRepo.new(db_path(*input), self.init_sql)
        end
      {% else %}
        class_getter db = ::Crorm::SQRepo.new(self.db_path, self.init_sql)
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

  def insert!(stmt : String = @@schema.insert_stmt,
              values = self.db_values,
              db = self.class.db)
    self.class.set_one(stmt, *values, db: db)
  end

  def upsert!(stmt : String = @@schema.upsert_stmt,
              values = self.db_values,
              db = self.class.db)
    self.class.set_one(stmt, *values, db: db)
  end

  def update!(stmt : String = @@schema.update_stmt,
              values = self.update_values,
              db = self.class.db)
    self.class.set_one(stmt, *values, db: db)
  end
end

# Crorm

Utilities to easy working with sqlite/postgresql databases.

## For postgresql

```crystal
class Model
  include Crorm::Model
  schema "table_name", :postgres, strict: false

  class_getter db : DB::Database { load_db_database_or_connection }

  # pkey: using for generate primary key field
  # auto: can be null, auto generated by db

  field id : Int32, pkey: true, auto: true

  field other_id : Int32, pkey: true # composite primary key

  field extra : String = "" # provide default value

  timestamps # create created_at and updated_at field
end
```

## For sqlite

```crystal
class Model
  include Crorm::Model
  schema "table_name", :sqlite, multi: true # or false

  class_getter init_sql <<-SQL
    create table table_name(...)
  SQL

  def self.db_path(*) # if using multi model
  end
end
```

## Query tools

```crystal
class Model
  def self.get_all(*args_, args : Array? = nil, db : DB_ = self.db, as as_type = self, &)

  def self.get_all(*args_, args : Array? = nil, db : DB_ = self.db, as as_type = self)

  def self.get_all_by_ids(ids : Enumerable, pkey : String = "id", db : DB_ = self.db, as as_type = self)

  def self.get(*args_, args : Array? = nil, db : DB_ = self.db, as as_type = self, &) : self | Nil

  def self.get!(*args_, args : Array? = nil, db : DB_ = self.db, as as_type = self, &) : self

  def self.get_by_pkey(id, pkey = "id", db : DB_ = self.db) : self | Nil

  def self.get_by_pkey!(id, pkey = "id", db : DB_ = self.db) : self
end

```

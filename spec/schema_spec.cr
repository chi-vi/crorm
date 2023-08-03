require "../src/crorm/schema"

schema = Crorm::Schema.new("test")

schema.db_fields << "id"
schema.pk_fields << "id"

schema.db_fields << "body"
schema.upsert_fields << "body"

schema.db_fields << "extra"
schema.upsert_fields << "extra"

puts schema.select_stmt
puts schema.insert_stmt
puts schema.update_stmt
puts schema.upsert_stmt
puts schema.delete_stmt

require "db"

struct Time
  def self.from_rs(rs : DB::ResultSet)
    unix(rs.read(Int64))
  end

  def self.to_db(value : self)
    value.to_unix
  end
end

struct Enum
  def self.from_rs(rs : DB::ResultSet)
    from_value(rs.read(Int64))
  end

  def self.to_db(value : self)
    value.value
  end
end

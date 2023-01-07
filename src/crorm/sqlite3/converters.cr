require "db"

struct Time
  def self.from_rs(rs : DB::ResultSet)
    unix(rs.read(Int64))
  end

  def to_db
    to_unix
  end
end

struct Enum
  def self.from_rs(rs : DB::ResultSet)
    from_value(rs.read(Int64))
  end

  def to_db
    value
  end
end

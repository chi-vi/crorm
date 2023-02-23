require "db"

class Object
  def self.from_rs(rs : DB::ResultSet)
    rs.read(self)
  end

  def to_db
    self
  end
end

struct Time
  def self.from_rs(rs : DB::ResultSet)
    self.unix(rs.read(Int64))
  end

  def to_db
    self.to_unix
  end
end

struct Enum
  def self.from_rs(rs : DB::ResultSet)
    from_value(rs.read(Int64))
  end

  def to_db
    @value
  end
end

struct UUID
  def self.from_rs(rs : DB::ResultSet)
    new(rs.read(Bytes))
  end

  def to_db
    @bytes.to_slice
  end
end

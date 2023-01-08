require "./spec_helper"

class Test
  include Crorm::Model
  field a : Int32
  field b : Int32? = 4
  field t : Time = Time.utc, converter: Time
end

describe "Crorm::Model" do
  it "can include modeel" do
    test = Test.new({a: 1})
    # puts test.get_changes
    test.a.should eq 1
    test.b.should eq 4
  end
end

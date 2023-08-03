require "./spec_helper"

class Test
  include Crorm::Model
  schema "test"

  field a : Int32, pkey: true, auto: true
  field b : Int32? = 4
end

describe "Crorm::Model" do
  it "can include model" do
    test = Test.new
    test.a = 1

    # puts test.get_changes
    test.a.should eq 1
    test.b.should eq 4

    test.pk_values.should eq({1})
    test.db_values.should eq({1, 4})

    test.update_values.should eq({4, 1})
  end
end

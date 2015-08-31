module Mapymo
  class Test
    include Mapymo::Object

    define_table('MapymoTest') do |t|
      t.hash_key :hash, :string
      t.range_key :range, :numeric
      t.string :string
      t.numeric :numeric
      t.binary :binary
      t.provisioned_throughput = { read_capacity_units: 1,
                                   write_capacity_units: 1 }
    end

  end
end

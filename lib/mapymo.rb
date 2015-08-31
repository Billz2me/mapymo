require 'active_support'
require 'active_model'

require 'aws-sdk-core'
require 'aws-sdk-resources'

# Thin DynamoDB SDK wrapper to make working with DynamoDB easier :).
module Mapymo
  extend ActiveSupport::Autoload

  eager_autoload do
    autoload :Error
    autoload :TableHelpers
    autoload :Finders
    autoload :Persistence
    autoload :Marshaler
  end

  # Mapymo.logger
  cattr_accessor :logger
  self.logger = Logger.new($stdout)

  # Mapymo.marshaler
  cattr_accessor :marshaler
  self.marshaler = Mapymo::Marshaler.new

  # Mapymo.aws_client
  cattr_accessor :aws_client
  self.aws_client = Aws::DynamoDB::Client.new

  # Example:
  #
  # class MyNeatObject
  #   include Mapymo::Object
  #
  #   define_table('MyNeatObjects') do |t|
  #     t.hash_key :my_hash_key, :string
  #     t.range_key :my_range_key, :myk
  #     t.provisioned_through = { read_capacity_units: 1, write_capacity_units: 1 }
  #   end
  # end
  #
  module Object
    extend ActiveSupport::Concern

    include ActiveModel::Model
    include ActiveModel::Validations

    include Mapymo::TableHelpers
    include Mapymo::Finders
    include Mapymo::Persistence
  end

end

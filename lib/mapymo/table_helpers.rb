module Mapymo
  module TableHelpers
    extend ActiveSupport::Concern

    included do
      cattr_accessor :table, :aws_client, :attribute_types
    end

    class_methods do
      # Public: Define table using ActiveRecord-esque syntax.
      # Example:
      #   define_table('TableName') do |t|
      #     t.hash_key :hash, :string
      #     t.range_key :range, :numeric
      #     t.string :string
      #     t.numeric :numeric
      #     t.binary :binary
      #     t.provisioned_throughput({ read: 1, write: 1 })
      #   end
      def define_table(table_name = self.name.delete("::").pluralize, &block)
        self.table = Aws::DynamoDB::Table.new(table_name)

        table_definition = TableDefinition.new(table_name)
        yield(table_definition)
        self.attribute_types = table_definition.attribute_types

        begin
          self.table.table_status
        rescue Aws::DynamoDB::Errors::ResourceNotFoundException => e
          Mapymo.logger.debug(e.message)
          result = Mapymo.aws_client.create_table(table_definition.to_hash)
          Mapymo.logger.debug(result.inspect)
        end
      end

      # Public: Build a primary key hash from a hash key and range key value.
      # Returns [Hash]
      def build_key(hash_key_value, range_key_value = nil)
        key = { hash_key.attribute_name => hash_key_value }
        if range_key
          key[range_key.attribute_name] = range_key_value
        end
        key
      end

      # Public: The hash key schema for this object.
      # Returns [Aws::DynamoDB::Types::KeySchemaElement]
      def hash_key(key_schema = table.key_schema)
        key_attr(key_schema, TableDefinition::HASH)
      end

      # Public: The range key schema for this object.
      # Returns [Aws::DynamoDB::Types::KeySchemaElement]
      def range_key(key_schema = table.key_schema)
        key_attr(key_schema, TableDefinition::RANGE)
      end

      # Public: Get a global secondary index definition by name.
      def global_secondary_index(index_name)
        indexes = table.global_secondary_indexes
        indexes.find {|index| index.index_name.eql?(index_name)} if indexes
      end

      # Public: Get a local secondary index definition by name.
      def local_secondary_index(index_name)
        indexes = table.local_secondary_indexes
        indexes.find {|index| index.index_name.eql?(index_name)} if indexes
      end

      # Public: Helper for finding the hash/range key in the key schema.
      def key_attr(key_schema, type)
        key_schema.find { |key| key.key_type.eql?(type) }
      end

    end#class_methods

    # Helper class intended to be used only by this module
    # To support defining the table in a migration-like syntax.
    class TableDefinition

      HASH = "HASH"
      RANGE = "RANGE"
      STRING = 'S'
      NUMERIC = 'N'
      BINARY = 'B'

      attr_accessor :attribute_definitions,
                    :attribute_types,
                    :table_name,
                    :key_schema,
                    :provisioned_throughput,
                    :local_secondary_indexes,
                    :global_secondary_indexes,
                    :stream_specification

      # Public: Construct a TableDefinition.
      def initialize(table_name)
        @table_name = table_name
        @attribute_definitions = []
        @key_schema = []
        @provisioned_throughput = {}
        @attribute_types = {}
      end

      # Public: Define the hash key attribute.
      # key - The attribute name
      # type - accepts [:string, :numeric, :binary]
      def hash_key(key, type)
        @key_schema << { attribute_name: key, key_type: HASH }
        @attribute_definitions << send(type, key)
      end

      # Public: Define the range key attribute.
      # key - The attribute name
      # type - accepts [:string, :numeric, :binary]
      def range_key(key, type)
        @key_schema << { attribute_name: key, key_type: RANGE }
        @attribute_definitions << send(type, key)
      end

      # Public: Define a string attribute.
      def string(key)
        define_attribute(key, STRING)
      end

      # Public: Define a numeric attribute.
      def numeric(key)
        define_attribute(key, NUMERIC)
      end

      # Public: Define a binary attribute.
      def binary(key)
        define_attribute(key, BINARY)
      end

      # Public: to hash implementation.
      def to_hash
        h = { attribute_definitions: @attribute_definitions,
              table_name: @table_name,
              key_schema: @key_schema,
              provisioned_throughput: @provisioned_throughput }
        h[:local_secondary_indexes] = @local_secondary_indexes if @local_secondary_indexes
        h[:global_secondary_indexes] = @global_secondary_indexes if @global_secondary_indexes
        h[:stream_specification] = @stream_specification if @stream_specification
        h
      end

      private

      # Internal: Helper method to define an attribute.
      def define_attribute(key, type)
        @attribute_types[key] = type
        { attribute_name: key, attribute_type: type}
      end

    end#TableDefinition

  end
end

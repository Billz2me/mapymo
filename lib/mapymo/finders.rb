module Mapymo
  module Finders
    extend ActiveSupport::Concern

    RANGE_ATTR_NAME = "#R"
    HASH_ATTR_NAME = "#H"
    HASH_VAL = ':hashKey'
    RANGE_VAL1 = ':rangeVal'
    RANGE_VAL2 = ':rangeVal2'

    # Mapymo::Finders::RecordNotFound
    class RecordNotFound < Error; end

    class_methods do

      # Public: Find an item by hash and (optional) range key.
      #
      # hash_key - The hash key of the item to find *required
      # range_key - The range key of the item to find
      # options - The same options passed to get_item.
      #
      # Returns an instance of object found.
      def find(hash_key, range_key = nil, options = {})
        item = table.get_item(options.merge({ key: build_key(hash_key, range_key) })).item
        Mapymo.marshaler.item_to_object(self, item) if item
      end

      # Public: .see #find
      def find!(hash_key, range_key = nil, options = {})
        found = find(hash_key, range_key, options)
        return found if found
        msg = "No record with hash_key: #{hash_key.inspect} and range_key: #{range_key.inspect} in table #{table.table_name}"
        raise RecordNotFound.new(msg)
      end

      # Public: Find all items with a given hash key.
      def find_all(hash_key)
        query(hash_key)
      end

      # Public: Perform a table query with a range key condition.
      #
      # hash_key - The hash key.
      # range_key_condition - Array with [operator, *values]
      #
      # Example:
      #    query('hkey', ['=>', Time.now.to_i])
      #    query('hkey', ['begins_with',
      #    query('hkey', ['between', 1, 5])
      #
      # Returns a list of results that match the query.
      def query(hash_key, range_key_condition = nil)
        perform_query(hash_key, range_key_condition)
      end

      # Public: Perform a query on an index.
      #
      # index_name - The name of the index to query.
      #
      # See .query
      def query_index(index_name, hash_key, range_key_condition = nil)
        index = local_secondary_index(index_name) || global_secondary_index(index_name)
        perform_query(hash_key, range_key_condition, index.key_schema, { index_name: index_name })
      end

      private

      # Internal: The magic.
      def perform_query(hash_key, range_key_condition = nil, key_schema = table.key_schema, options = {})
        key_condition_expression = "#{HASH_ATTR_NAME} = #{HASH_VAL}"
        expression_attribute_names = { HASH_ATTR_NAME  => self.hash_key(key_schema).attribute_name }
        expression_attribute_values = { HASH_VAL => hash_key }

        if range_key_condition
          operator, *range_values = range_key_condition
          key_condition_expression += " AND #{build_range_key_expression(operator, range_values)}"

          expression_attribute_names[RANGE_ATTR_NAME] = self.range_key(key_schema).attribute_name
          expression_attribute_values[RANGE_VAL1] = range_values[0]
          expression_attribute_values[RANGE_VAL2] = range_values[1] if range_values[1]
        end

        Mapymo.logger.debug "Querying #{key_condition_expression}, #{expression_attribute_names}, #{expression_attribute_values}"

        items = table.query(options.merge(key_condition_expression: key_condition_expression,
                              expression_attribute_names: expression_attribute_names,
                              expression_attribute_values: expression_attribute_values)).items

        items.map { |i| Mapymo.marshaler.item_to_object(self, i) }
      end

      # Internal: Helper method to build the range key expression for a query operation.
      # Returns the range key expression string.
      def build_range_key_expression(operator, values)
        raise ArgumentError.new("Must supply query substitution values") unless values.size > 0

        case operator.to_s
        when 'between'
          raise ArgumentError.new("Must provide 2 values with between operator") unless values.size.eql?(2)
          "#{RANGE_ATTR_NAME} BETWEEN #{RANGE_VAL1} and #{RANGE_VAL2}"
        when 'begins_with'
          "begins_with(#{RANGE_ATTR_NAME}, #{RANGE_VAL1})"
        when '=', '<', '<=', '>', '>='
          "#{RANGE_ATTR_NAME} #{operator} #{RANGE_VAL1}"
        else
          raise ArgumentError.new("Invalid query operator #{operator}")
        end
      end

    end#class_methods

  end
end

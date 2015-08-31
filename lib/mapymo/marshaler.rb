module Mapymo
  class Marshaler

    # Public: Marshal an object into an item.
    #
    # object - The object to marshal into an item.
    #
    def object_to_item(object)
      Mapymo.logger.debug "Marshaling object #{object.inspect} into item"
      result = object.class.attribute_types.each_with_object({}) do |(attr_name, attr_type), item|
        obj_value = object.send(attr_name)
        if obj_value
          item[attr_name] = marshal(obj_value, attr_type)
        end
      end
      Mapymo.logger.debug(result.inspect)
      result
    end

    # Public: Unmarshal an item into an object.
    #
    # klazz - The object class
    # item - The item to marshal
    #
    # Returns an instance of klazz.
    def item_to_object(klazz, item)
      Mapymo.logger.debug "Unmarshaling item #{item} into #{klazz}"
      obj_attrs = klazz.attribute_types.each_with_object({}) do |(attr_name, attr_type), hash|
        item_value = item[attr_name.to_s]
        if item_value
          hash[attr_name] = unmarshal(item_value, attr_type)
        end
      end
      result = klazz.new(obj_attrs)
      Mapymo.logger.debug(result.inspect)
      result
    end

    private

    # Internal: Marshal value/type.
    def marshal(value, type)
      value
    end

    # Internal: Unmarshal value/type.
    def unmarshal(value, type)
      value
    end

  end
end

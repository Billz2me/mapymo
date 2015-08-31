module Mapymo
  module Persistence
    extend ActiveSupport::Concern

    # Mapymo::Persistence::RecordNotSaved
    class RecordNotSaved < Error; end

    # Public: Save this object.
    #
    # options - put_item options
    #
    # Returns true/false
    def save(options = {})
      item = Mapymo.marshaler.object_to_item(self)
      valid? && put_item(item, options)
    end

    # Public: .see #save
    def save!(options = {})
      item = Mapymo.marshaler.object_to_item(self)
      if valid?
        put_item(item, options, true)
      else
        raise RecordNotSaved.new('Record failed validations')
      end
    end

    # Internal: Put item into the table.
    #
    # item - The item to put.
    # options - The put item options.
    # raise_errors - boolean indicating whether to reraise errors.
    #
    # Returns true/false indicating if the item was saved.
    def put_item(item, options = {}, raise_errors = false)
      begin
        table.put_item(options.merge(item: item))
        true
      rescue => e
        raise RecordNotSaved.new(e) if raise_errors
        false
      end
    end

  end
end


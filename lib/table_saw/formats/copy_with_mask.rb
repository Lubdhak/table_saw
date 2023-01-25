# frozen_string_literal: true

module TableSaw
  module Formats
    class CopyWithMask < TableSaw::Formats::Base

      def header
        "COPY #{table_name} (#{quoted_columns}) FROM STDIN;"
      end

      def footer
        ['\.', "\n"]
      end

      # complexity ~ Σ(masked_column_count × associated_sequences_count)
      def dump_row(row)
        return row unless block_given?
        mask_config = yield
        return row if mask_config.nil?
        columns = columns_to_mask(mask_config)
        return row if columns.select{|col| col != nil}.empty?
        column_contents = row.split(/\t/)
        if column_contents.count != columns.count
          puts "row=#{row}"
          raise "String Parse Error"
        end
        columns.each_with_index do |sequence_hash,idx|
          next if sequence_hash.nil?
          sequence_hash.each do |rgx_find_key, replace_str_val|
            next if rgx_find_key.empty? || replace_str_val.empty?
            pattern = Regexp.new(rgx_find_key.to_str)
            column_contents[idx] = column_contents[idx].sub(/#{pattern}/,replace_str_val)
          end
        end
        column_contents.join("\t")
      end

      private


      def columns_to_mask(mask_config)
        TableSaw.schema_cache.columns_hash(table_name).keys.map{|col_name| mask_config.fetch(col_name, nil) }
      end

      def quoted_columns
        TableSaw.schema_cache.columns_hash(table_name)
          .each_key
          .map { |name| connection.quote_column_name(name) }
          .join(', ')
      end

      def connection
        TableSaw.schema_cache.connection
      end
    end
  end
end
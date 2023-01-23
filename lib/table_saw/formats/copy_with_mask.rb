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
        columns = columns_to_mask(mask_config)
        column_content = row.gsub("\n","").split(/\t/)
        raise "String Parse Error" if column_content.count != columns.count

        columns.each_with_index do |sequences,idx|
          next if sequences.nil?
          sequences.each do |rgx_find, replace_str|
            next if rgx_find.empty? || replace_str.empty?
            pattern = Regexp.new(rgx_find.to_str)
            column_content[idx] = column_content[idx].gsub(/#{pattern}/,replace_str)
          end
        end

        column_content.join("\t") + "\n"
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
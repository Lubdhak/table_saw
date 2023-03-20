# frozen_string_literal: true

require 'fileutils'

module TableSaw
  class CreateDumpFile
    attr_reader :records, :file, :format, :data_arr

    FORMATS = {
      'copy' => TableSaw::Formats::Copy,
      'insert' => TableSaw::Formats::Insert,
      'copy_with_mask' => TableSaw::Formats::CopyWithMask
    }.freeze

    def initialize(records, output:, format:)
      @records = records
      @file = output
      @format = format
      @data_arr = []
    end

    # rubocop:disable Metrics/MethodLength,Metrics/AbcSize
    def call
      puts "CreateDumpFile.call"
      File.delete(file) if File.exist?(file)
      FileUtils.mkdir_p(File.dirname(file))
      puts "created files call"

      alter_constraints_deferrability

      write_to_file <<~SQL
        BEGIN;

        SET statement_timeout = 0;
        SET lock_timeout = 0;
        SET client_encoding = 'UTF8';
        SET standard_conforming_strings = on;
        SET check_function_bodies = false;
        SET client_min_messages = warning;

        SET search_path = public, pg_catalog;
      SQL

      records.each do |name, table|
        puts "starting for table #{table}"
        defer_constraints(name)

        write_to_file <<~COMMENT
          --
          -- Data for Name: #{name}; Type: TABLE DATA
          --

        COMMENT

        formatter = FORMATS.fetch(format.fetch('type', 'copy'), TableSaw::Formats::Copy).new(name, options: format)
        Array(formatter.header).each { |line| write_to_file(line) }

        TableSaw::Connection.with do |conn|
          conn.copy_data "COPY (#{table.copy_statement}) TO STDOUT", formatter.coder do
            while (row = conn.get_copy_data)
              puts "trying to fetch data"
              write_to_file formatter.dump_row(row){ mask_columns(table, name) }
            end
            actual_write_to_file
            puts "starting GC"
            GC.start
            puts "#{table}"
          end
        end

        Array(formatter.footer).each { |line| write_to_file(line) }
      end

      write_to_file 'COMMIT;'
      write_to_file "\n"

      refresh_materialized_views
      restart_sequences

      alter_constraints_deferrability keyword: 'NOT DEFERRABLE'
      actual_write_to_file
    end
    # rubocop:enable Metrics/MethodLength,Metrics/AbcSize

    private

    def actual_write_to_file
      puts "array size = #{@data_arr.count}"
      File.open(file, 'ab') do |f|
        puts "writing to file..."
        @data_arr.each do |data| f.write(data) end
        f.close
      end
      @data_arr = []
    end

    def mask_columns(table, name)
      table.manifest.tables[name].respond_to?('mask_columns') ? table.manifest.tables[name].mask_columns : nil
    end

    def alter_constraints_deferrability(keyword: 'DEFERRABLE')
      records.each_key do |name|
        write_to_file <<~COMMENT
          --
          -- Alter Constraints for Name: #{name}; Type: #{keyword}
          --

        COMMENT

        TableSaw.information_schema.constraint_names[name].each do |constraint_name|
          write_to_file "ALTER TABLE #{name} ALTER CONSTRAINT #{constraint_name} #{keyword};"
        end
      end
    end

    def defer_constraints(name)
      write_to_file <<~COMMENT
        --
        -- Set Constraints for Name: #{name}; Type: DEFERRED
        --

      COMMENT

      TableSaw.information_schema.constraint_names[name].each do |constraint_name|
        write_to_file "SET CONSTRAINTS #{constraint_name} DEFERRED;"
      end
    end

    def refresh_materialized_views
      TableSaw::Queries::MaterializedViews.new.call.each do |view|
        write_to_file "refresh materialized view #{view};"
      end

      write_to_file "\n"
    end

    def restart_sequences
      TableSaw::Queries::SerialSequences.new.call.slice(*records.keys).each do |table, sequence|
        write_to_file <<~SQL
          select setval('#{sequence.name}', (select max(#{sequence.column}) from #{table}), true);
        SQL
      end

      write_to_file "\n"
    end

    def write_to_file(data)
      @data_arr << data
    end
  end
end

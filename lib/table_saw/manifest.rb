# frozen_string_literal: true

require 'yaml'
require 'table_saw/associations'

module TableSaw
  class Manifest
    class Table
      attr_reader :variables, :config

      def initialize(variables, config)
        @variables = variables
        @config = config
      end

      def table
        config['table']
      end

      alias name table

      def query
        return unless partial?

        format(config['query'], variables.transform_keys(&:to_sym))
      end

      def partial?
        config.key?('query')
      end

      def has_many
        config.fetch('has_many', [])
      end

      def mask_columns
        @mask_columns ||= config.fetch('mask_columns',nil)
      end

    end

    def self.instance
      raise ArgumentError, 'Could not find manifest file' unless File.exist?(TableSaw.configuration.manifest)

      new(YAML.safe_load(File.read(TableSaw.configuration.manifest)))
    end

    attr_reader :config

    def initialize(config)
      @config = config
    end

    def variables
      config.fetch('variables', {})
    end

    def tables
      @tables ||= config['tables'].map { |entry| Table.new(variables, entry) }.each_with_object({}) do |t, memo|
        memo[t.name] = t
      end
    end

    def has_many
      @has_many ||= config.fetch('has_many', {})
    end

    def foreign_keys
      @foreign_keys ||= config.fetch('foreign_keys', [])
    end

    def associations
      @associations ||= TableSaw::Associations.new(self)
    end
  end
end

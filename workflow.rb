require 'rbbt'
require 'rbbt/workflow'
require 'rbbt/sources/organism'

module DbSNP
  extend Workflow

  class << self
    attr_accessor :organism
  end

  self.organism = "Hsa/jan2013"

  input :mutations, :array, "Genomic Mutation", nil, :stream => true
  input :by_position, :boolean, "Identify by position", false
  input :organism, :select, "Organism code", Organism.default_code("Hsa"), :select_options => %w(GRCh37 GRCh38)
  input :dbsnp_set, :select, "How many DbSNP mutations to use", "common", :select_options => %w(common all)
  task :identify => :tsv do |mutations,by_position,organism,set|
    dumper = TSV::Dumper.new :key_field => "Genomic Mutation", :fields => ["RS ID"], :type => :single
    dumper.init
    build = Organism.GRC_build(organism)
    raise ParameterException, "Organism build #{build} unkown" if build.nil?
    database = DbSNP.database(build, set)
    database.unnamed = true
    TSV.traverse mutations, :into => dumper, :bar => self.progress_bar("Identify dbSNP"), :type => :array do |mutation|
      next if mutation.empty?
      if by_position
        position = mutation.split(":")[0..1] * ":"
        matches = database.prefix(position+":")
        rsid = database.chunked_values_at matches
      else
        rsid = database[mutation]
      end
      next if rsid.nil?
      [mutation, rsid]
    end
  end


  dep :identify
  task :annotate => :tsv do 
    database = DbSNP.rsid_database
    dumper = TSV::Dumper.new :key_field => "Genomic Mutation", :fields => ["RS ID"] + database.fields[1..-1], :type => :list
    dumper.init
    database.unnamed = true
    TSV.traverse step(:identify), :into => dumper, :bar => self.progress_bar("Annotate dbSNP") do |mutation, rsid|
      next if mutation.empty?
      values = database[rsid]
      next if values.nil?
      values[0] = rsid
      [mutation, values]
    end
  end
end

require 'rbbt/sources/dbSNP'

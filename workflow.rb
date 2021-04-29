require 'rbbt'
require 'rbbt/workflow'

module DbSNP
  extend Workflow

  class << self
    attr_accessor :organism
  end

  self.organism = "Hsa/feb2014"

  input :mutations, :array, "Genomic Mutation", nil, :stream => true
  input :by_position, :boolean, "Identify by position", false
  task :identify => :tsv do |mutations,by_position|
    dumper = TSV::Dumper.new :key_field => "Genomic Mutation", :fields => ["RS ID"], :type => :single
    dumper.init
    database = DbSNP.database
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

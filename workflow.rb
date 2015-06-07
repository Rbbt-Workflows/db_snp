require 'rbbt'
require 'rbbt/workflow'

module DbSNP
  extend Workflow

  class << self
    attr_accessor :organism
  end

  self.organism = "Hsa/jan2013"

  input :mutations, :array, "Genomic Mutation", nil, :stream => true
  task :identify => :tsv do |mutations|
    dumper = TSV::Dumper.new :key_field => "Genomic Mutation", :fields => ["RS ID"], :type => :single
    dumper.init
    database = DbSNP.database
    database.unnamed = true
    TSV.traverse mutations, :into => dumper, :bar => self.progress_bar("Identify dbSNP"), :type => :array do |mutation|
      next if mutation.empty?
      rsid = database[mutation]
      next if rsid.nil?
      [mutation, rsid]
    end
  end


  dep :identify
  task :annotate => :tsv do 
    database = DbSNP.rsid_database
    dumper = TSV::Dumper.new :key_field => "Genomic Mutation", :fields => ["RS ID"] + database.fields[1..-1], :type => :single
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

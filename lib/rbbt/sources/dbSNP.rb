require 'rbbt-util'
require 'rbbt/util/open'
require 'rbbt/resource'
require 'rbbt/persist'
require 'net/ftp'

$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '../../..', 'lib'))

module DbSNP
  extend Resource
  self.subdir = "share/databases/dbSNP"

  class << self
    attr_accessor :organism
  end

  self.organism = "Hsa/jan2013"

  NCBI_URL = "ftp://ftp.ncbi.nlm.nih.gov/snp/organisms/human_9606_b146_GRCh37p13/VCF/00-All.vcf.gz"

  DbSNP.claim DbSNP.mutations, :proc do |filename|
    Open.write filename do |file|
      file.puts <<-EOF
#: :namespace=#{DbSNP.organism}#:type=:flat
#RS ID\tGenomic Mutation
      EOF
      Open.read(NCBI_URL, :nocache => true) do |line|
        next if line[0] == "#"

        chr, pos, id, ref, alt, qual, filter, info = line.split("\t")
        pos, alts = Misc.correct_vcf_mutation(pos.to_i, ref, alt) 

        muts = alts.collect do |alt|
          [chr, pos, alt] * ":"
        end

        file.puts [id, muts].flatten * "\t"
      end
    end
    nil
  end

  DbSNP.claim DbSNP.rsids, :proc do
    Workflow.require_workflow "Sequence"
    TSV.reorder_stream(Sequence::VCF.open_stream(Open.open(NCBI_URL, :nocache => true), false, false, true), {0 => 2})
  end

  GM_SHARD_FUNCTION = Proc.new do |key|
    key[0..key.index(":")-1]
  end

  RS_SHARD_FUNCTION = Proc.new do |key|
    key[-2..-1]
  end

  CHR_POS = Proc.new do |key|
    raise "Key (position) not String: #{ key }" unless String === key
    if match = key.match(/.*?:(\d+):?/)
      match[1].to_i
    else
      raise "Key (position) not understood: #{ key }"
    end
  end

  def self.database
    @@database ||= begin
                     db = Persist.persist_tsv("dbSNP", DbSNP.mutations, {}, :persist => true,
                                         :file => Rbbt.var.DbSNP.shard_mutations.find,
                                         :prefix => "dbSNP", :serializer => :string, :engine => "BDB",
                                         :shard_function => GM_SHARD_FUNCTION, :pos_function => CHR_POS) do |sharder|
                       sharder.fields = ["RS ID"]
                       sharder.key_field = "Genomic Position"
                       sharder.type = :single

                       TSV.traverse DbSNP.mutations, :type => :array, :into => sharder, :bar => "Processing DbSNP" do |line|
                         next if line =~ /^#/
                         rsid, *mutations = line.split "\t"

                         res = mutations.collect do |mutation|
                           [mutation, rsid]
                         end

                         res.extend MultipleResult

                         res
                       end
                      end
                     db.unnamed = true
                     db
                    end
  end


  #def self.rsid_database
  #  @@rsid_database ||= begin
  #                        db = DbSNP.rsids.tsv :persist => true, :persist_file => Rbbt.var.DbSNP.rsids.find, :monitor => true, :unnamed => true
  #                        db.unnamed = true
  #                        db
  #                      end
  #end

  def self.rsid_database
    @@rsid_database ||= begin
                     db = Persist.persist_tsv("dbSNP", DbSNP.rsids, {}, :persist => true,
                                         :file => Rbbt.var.DbSNP.shard_rsids.find,
                                         :prefix => "dbSNP", :serializer => :list, :engine => "HDB:big",
                                         :shard_function => RS_SHARD_FUNCTION) do |sharder|
                       key_field, *fields = TSV.parse_header(DbSNP.rsids).all_fields
                       sharder.fields = fields
                       sharder.key_field = key_field
                       sharder.type = :list

                       TSV.traverse DbSNP.rsids, :type => :array, :into => sharder, :bar => "Processing DbSNP rsids" do |line|
                         next if line =~ /^#/
                         key, *values = line.split("\t")
                         [key, values]
                       end
                      end
                     db.unnamed = true
                     db
                    end

  end

  def self.caf_database
    @@database ||= begin
                     db = Persist.persist_tsv("dbSNP", DbSNP.rsids, {}, :persist => true,
                                         :file => Rbbt.var.DbSNP.shard_caf.find,
                                         :prefix => "dbSNP", :serializer => :flat, :engine => "HDB:big",
                                         :shard_function => RS_SHARD_FUNCTION) do |sharder|
                       key_field, *fields = TSV.parse_header(DbSNP.rsids).all_fields
                       pos = fields.index "CAF"
                       sharder.fields = ["CAF"]
                       sharder.key_field = key_field
                       sharder.type = :flat

                       TSV.traverse DbSNP.rsids, :type => :array, :into => sharder, :bar => "Processing DbSNP rsids" do |line|
                         next if line =~ /^#/
                         key, *values = line.split("\t")
                         caf = values[pos].split(",")
                         [key, caf]
                       end
                      end
                     db.unnamed = true
                     db
                    end

  end

  DbSNP.claim Rbbt.var.DbSNP.shard_mutations, :proc do
    DbSNP.database
    nil
  end


  DbSNP.claim Rbbt.var.DbSNP.rsids, :proc do
    DbSNP.rsid_database
    nil
  end
end

require 'rbbt/sources/dbSNP/indices'

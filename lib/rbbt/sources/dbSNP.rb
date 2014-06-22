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

  NCBI_URL = "ftp://ftp.ncbi.nlm.nih.gov/snp/organisms/human_9606_b141_GRCh37p13/VCF/common_all.vcf.gz"

  DbSNP.claim DbSNP.mutations, :proc do |filename|
    Open.write filename do |file|
      file.puts <<-EOF
#: :namespace=#{DbSNP.organism}#:type=:flat
#RS ID\tGenomic Mutation
      EOF
      Open.read(NCBI_URL) do |line|
        next if line[0] == "#"

        chr, pos, id, ref, alt, qual, filter, info = line.split("\t")
        pos, alt = Misc.correct_vcf_mutation(pos.to_i, ref, alt) 

        mutation = [chr, pos, alt] * ":"
        file.puts [id, mutation] * "\t"
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
                     Persist.persist_tsv("dbSNP", DbSNP.mutations, {}, :persist => true,
                                         :file => Rbbt.var.DbSNP.shard_mutations,
                                         :prefix => "dbSNP", :serializer => :string, :engine => "HDB",
                                         :shard_function => GM_SHARD_FUNCTION, :pos_function => CHR_POS) do |sharder|
                       sharder.fields = ["RS ID"]
                       sharder.key_field = "Genomic Position"
                       sharder.type = :single

                       TSV.traverse DbSNP.mutations, :type => :array, :into => sharder, :bar => "Processing DbSNP" do |line|
                         next if line =~ /^#/
                         rsid,_sep, mutation = line.partition "\t"
                         [mutation, rsid]
                       end
                      end
                    end
  end

  def self.rsid_database
    @@rsid_database ||= begin
                     DbSNP.rsids.tsv :persist => true, :persist_file => Rbbt.var.DbSNP.rsids
                    end
  end

  DbSNP.claim Rbbt.var.DbSNP.shard_mutations, :proc do
    DbSNP.database
    nil
  end


  DbSNP.claim Rbbt.var.DbSNP.shard_mutations, :proc do
    DbSNP.rsid_database
    nil
  end
end

require 'rbbt/sources/dbSNP/indices'
require 'rbbt/sources/dbSNP/entity'

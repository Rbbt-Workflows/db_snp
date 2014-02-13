require 'rbbt'
require 'rbbt/util/open'
require 'rbbt/resource'
require 'net/ftp'

module DbSNP
  extend Resource
  self.subdir = "share/databases/dbSNP"
  class << self
    attr_accessor :organism
  end

  self.organism = "Hsa/jan2013"

  NCBI_URL = "ftp://ftp.ncbi.nlm.nih.gov/snp/organisms/human_9606/VCF/common_all.vcf.gz"

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
    Workflow.require_workflow "Genomics"
    require 'rbbt/entity/genomic_mutation'
    TSV.reorder_stream(GenomicMutation::VCF.open_stream(Open.open(NCBI_URL)), {0 => 1})
  end

  #DbSNP.claim DbSNP.mutations, :proc do
  #  tsv = TSV.setup({}, :key_field => "RS ID", :fields => ["Genomic Mutation"], :type => :flat)
  #  file = Open.open(NCBI_URL, :nocache => true) 
  #  while line = file.gets do
  #    next if line[0] == "#"[0]
  #    chr, position, id, ref, alt = line.split "\t"

  #    mutations = alt.split(",").collect do |a|
  #      if alt[0] == ref[0]
  #        alt[0] = '+'[0]
  #      end
  #      [chr, position, alt] * ":"
  #    end

  #    tsv.namespace = "Hsa/may2012"
  #    tsv[id] = mutations
  #  end

  #  tsv.to_s
  #end

  #DbSNP.claim DbSNP.rsids, :proc do |filename|
  #  ftp = Net::FTP.new('ftp.broadinstitute.org')
  #  ftp.passive = true
  #  ftp.login('gsapubftp-anonymous', 'devnull@nomail.org')
  #  ftp.chdir('/bundle/2.3/hg19')

  #  tmpfile = TmpFile.tmp_file + '.gz'
  #  ftp.getbinaryfile('dbsnp_137.hg19.vcf.gz', tmpfile, 1024)

  #  file = Open.open(tmpfile, :nocache => true) 
  #  begin
  #    File.open(filename, 'w') do |f|
  #      f.puts "#: :type=:list#:namespace=Hsa/may2012"
  #      f.puts "#" + ["RS ID", "GMAF", "G5", "G5A", "dbSNP Build ID"] * "\t"
  #      while line = file.gets do
  #        next if line[0] == "#"[0]

  #        chr, position, id, ref, muts, qual, filter, info = line.split "\t"

  #        g5 = g5a = dbsnp_build_id = gmaf = nil

  #        gmaf = $1 if info =~ /GMAF=([0-9.]+)/
  #        g5 = true if info =~ /\bG5\b/
  #        g5a = true if info =~ /\bG5A\b/
  #        dbsnp_build_id = $1 if info =~ /dbSNPBuildID=(\d+)/

  #        f.puts [id, gmaf, g5, g5a, dbsnp_build_id] * "\t"
  #      end
  #    end
  #  rescue Exception
  #    FileUtils.rm filename if File.exists? filename
  #    raise $!
  #  ensure
  #    file.close
  #    FileUtils.rm tmpfile
  #  end

  #  nil
  #end

  #DbSNP.claim DbSNP.mutations, :proc do |filename|
  #  ftp = Net::FTP.new('ftp.broadinstitute.org')
  #  ftp.passive = true
  #  ftp.login('gsapubftp-anonymous', 'devnull@nomail.org')
  #  ftp.chdir('/bundle/2.3/hg19')

  #  tmpfile = TmpFile.tmp_file + '.gz'
  #  ftp.getbinaryfile('dbsnp_137.hg19.vcf.gz', tmpfile, 1024)

  #  file = Open.open(tmpfile, :nocache => true) 
  #  begin
  #    File.open(filename, 'w') do |f|
  #      f.puts "#: :type=:flat#:namespace=Hsa/may2012"
  #      f.puts "#" + ["RS ID", "Genomic Mutation"] * "\t"
  #      while line = file.gets do
  #        next if line[0] == "#"[0]

  #        chr, position, id, ref, muts, qual, filter, info = line.split "\t"

  #        chr.sub!('chr', '')

  #        position, muts = Misc.correct_vcf_mutation(position.to_i, ref, muts)

  #        mutations = muts.collect{|mut| [chr, position, mut] * ":" }

  #        f.puts ([id] + mutations) * "\t"
  #      end
  #    end
  #  rescue Exception
  #    FileUtils.rm filename if File.exists? filename
  #    raise $!
  #  ensure
  #    file.close
  #    FileUtils.rm tmpfile
  #  end

  #  nil
  #end

  #DbSNP.claim DbSNP.mutations_hg18, :proc do |filename|
  #  require 'rbbt/sources/organism'

  #  mutations = CMD.cmd("grep -v '^#'|cut -f 2|sort -u", :in => DbSNP.mutations.open).read.split("\n").collect{|l| l.split("|")}.flatten

  #  translations = Misc.process_to_hash(mutations){|mutations| Organism.liftOver(mutations, "Hsa/jun2011", "Hsa/may2009")}
  #  begin
  #    file = Open.open(DbSNP.mutations.find, :nocache => true) 
  #    File.open(filename, 'w') do |f|
  #      f.puts "#: :type=:flat#:namespace=Hsa/may2009"
  #      f.puts "#" + ["RS ID", "Genomic Mutation"] * "\t"
  #      while line = file.gets do
  #        next if line[0] == "#"[0]
  #        parts = line.split("\t")
  #        parts[1..-1] = parts[1..-1].collect{|p| translations[p]} * "|"
  #        f.puts parts * "\t"
  #      end
  #    end
  #  rescue Exception
  #    FileUtils.rm filename if File.exists? filename
  #    raise $!
  #  ensure
  #    file.close
  #  end

  #  nil
  #end

end

require 'rbbt/sources/dbSNP/indices'
require 'rbbt/sources/dbSNP/entity'


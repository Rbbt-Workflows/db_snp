require 'rbbt/sources/dbSNP/indices'

if defined? Entity
  if defined? Gene and Entity === Gene
    module Gene
      property :dbSNP_rsids => :single2array do
        DbSNP.rsid_index(organism, chromosome)[self.chr_range]
      end

      property :dbSNP_mutations => :single2array do
        GenomicMutation.setup(DbSNP.mutation_index(organism).values_at(*self.dbSNP_rsids).compact.flatten.uniq, "dbSNP mutations over #{self.name || self}", organism, true)
      end
    end
  end

  if defined? GenomicMutation and Entity === GenomicMutation
    module GenomicMutation
      property :dbSNP => :array2single do
        dbSNP.mutations.tsv(:persist => true, :key_field => "Genomic Mutation", :fields => ["RS ID"], :type => :single).values_at *self
      end
    end
  end
end

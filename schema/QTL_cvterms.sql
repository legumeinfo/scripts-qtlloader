-------------------------------------------------------------------------------
-- databases required by QTL data                                              --
-------------------------------------------------------------------------------
INSERT INTO chado.db
  (name, description, url, urlprefix)
VALUES
  ('DOI', 
   'Document Object Identifier', 
   'http://www.doi.org/',  
   'http://dx.doi.org/'),
  ('LegumeInfo:traits', 
   'Trait names defined by LegumeInfo',
   '',
   ''
  ),
  ('LIS:cmap',
   'CMap at LIS',
   '',
   'http://cmap.comparative-legumes.org/cgi-bin/cmap/viewer'
  ),
  ('PMID',
   'PubMed ID',
   'http://www.ncbi.nlm.nih.gov/pubmed/',
   'http://www.ncbi.nlm.nih.gov/pubmed/'),
  ('SoyBase', 
   'Web resource for Glycine Max (soybean)', 
   'http://soybase.org/', 
   'http://soybase.org/'),
  ('genbank:gss', 
   'GenBank Genomic Sequence Survey database',
   'http://www.ncbi.nlm.nih.gov',
   'http://www.ncbi.nlm.nih.gov/gss/'),
  ('genbank:nuccore', 
   'GenBank nucleotide collection',
   'http://www.ncbi.nlm.nih.gov',
   'http://www.ncbi.nlm.nih.gov/nuccore/'),
  ('genbank:probe', 
   'GenBank Probe database',
   'http://www.ncbi.nlm.nih.gov',
   'http://www.ncbi.nlm.nih.gov/probe/'),
  ('genbank:taxonomy', 
   'GenBank taxonomy',
   'http://www.ncbi.nlm.nih.gov',
   'http://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id='),
  ('uniprot:species',
   'Uniprot species mnemonics',
   'http://www.uniprot.org/docs/speclist',
   ''),

--are these still needed?  
  ('download:map',
   'Download location for genetic maps',
   '/files/maps',
   '/files/maps'),
  ('download:QTL',
   'Download location for QTL data',
   '/files/qtls',
   '/files/qtls'),
  ('download:traits',
   'Download location for trait data',
   '/files/qtls',
   '/files/qtls')
;

UPDATE chado.db SET 
  description='Legume ontology', 
  url='http://soybase.org/ontology.php', 
  urlprefix='http://soybase.org/amigo/cgi-bin/SOY/go.cgi?search_constraint=terms&query='
WHERE name='SOY';



-------------------------------------------------------------------------------
-- controlled vocabularies required by QTL data                              --
-------------------------------------------------------------------------------
INSERT INTO chado.cv
  (name, definition)
VALUES
  ('uniprot_species', 
   'UniProt controlled species vocabulary. See http://www.uniprot.org/docs/speclist'),
   
  ('LegumeInfo:traits', 'Trait names defined by LegumeInfo')
;



-------------------------------------------------------------------------------
-- cvterms required by QTL data                                              --
-------------------------------------------------------------------------------

-- Add to local/internal
INSERT INTO chado.dbxref
  (db_id, accession)
VALUES
  ((SELECT db_id FROM db WHERE name='internal'), 'qtl_experiment'),
  ((SELECT db_id FROM db WHERE name='internal'), 'qtl_symbol'),
  ((SELECT db_id FROM db WHERE name='internal'), 'favorable_allele_source'),
  ((SELECT db_id FROM db WHERE name='internal'), 'has_trait'),
  ((SELECT db_id FROM db WHERE name='internal'), 'has_trait_class'),
  ((SELECT db_id FROM db WHERE name='internal'), 'has_obo_term'),
  
  ((SELECT db_id FROM db WHERE name='internal'), 'source_description'),
  ((SELECT db_id FROM db WHERE name='internal'), 'species_developed_in'),
  ((SELECT db_id FROM db WHERE name='internal'), 'source_marker'),
  ((SELECT db_id FROM db WHERE name='internal'), 'repeat_motif'),
  ((SELECT db_id FROM db WHERE name='internal'), 'restriction_enzyme'),
  ((SELECT db_id FROM db WHERE name='internal'), 'product_length'),
  ((SELECT db_id FROM db WHERE name='internal'), 'max_length'),
  ((SELECT db_id FROM db WHERE name='internal'), 'min_length'),
  ((SELECT db_id FROM db WHERE name='internal'), 'PCR_condition'),
  ((SELECT db_id FROM db WHERE name='internal'), 'sequence_name'),
  ((SELECT db_id FROM db WHERE name='internal'), 'SNP_alleles'),
  ((SELECT db_id FROM db WHERE name='internal'), 'SNP_five_prime_flanking_sequence'),
  ((SELECT db_id FROM db WHERE name='internal'), 'SNP_three_prime_flanking_sequence'),
  
  ((SELECT db_id FROM db WHERE name='internal'), 'canonical_marker'),
  ((SELECT db_id FROM db WHERE name='internal'), 'browser_track_name')
;

INSERT INTO chado.cvterm
  (cv_id, name, definition, dbxref_id)
VALUES
  ((SELECT cv_id FROM cv WHERE name='local'),
   'QTL Experiment', 'Indicates a data object of type QTL experiment', 
   (SELECT dbxref_id FROM dbxref WHERE accession='qtl_experiment')),
   
  ((SELECT cv_id FROM cv WHERE name='local'),
   'QTL Symbol', 'Indicates a data object is a QTL symbol', 
   (SELECT dbxref_id FROM dbxref WHERE accession='qtl_symbol')),
   
  ((SELECT cv_id FROM cv WHERE name='local'),
   'Favorable Allele Source', 'Indicates that a stock holds the favorable allele for a given QTL', 
   (SELECT dbxref_id FROM dbxref WHERE accession='favorable_allele_source')),
   
  ((SELECT cv_id FROM cv WHERE name='local'),
   'Has Trait Name', 'Indicates that a QTL symbol is associated with this trait', 
   (SELECT dbxref_id FROM dbxref WHERE accession='has_trait')),

  ((SELECT cv_id FROM cv WHERE name='local'),
   'Has Trait Class', 'Indicates that a QTL symbol is associated with this class', 
   (SELECT dbxref_id FROM dbxref WHERE accession='has_trait_class')),

  ((SELECT cv_id FROM cv WHERE name='local'),
   'Has OBO Term', 'Indicates that a formal OBO term is associated with this class', 
   (SELECT dbxref_id FROM dbxref WHERE accession='has_obo_term')),
   
  ((SELECT cv_id FROM cv WHERE name='local'),
   'Source Description', 'Description of the marker source.', 
   (SELECT dbxref_id FROM dbxref WHERE accession='source_description')),
  ((SELECT cv_id FROM cv WHERE name='local'),
   'Species Developed In', 'Species used to develop the marker.',
   (SELECT dbxref_id FROM dbxref WHERE accession='species_developed_in')),

  ((SELECT cv_id FROM cv WHERE name='local'),
   'Source Marker', 'Name of marker this marker was developed from (e.g. an aflp)', 
   (SELECT dbxref_id FROM dbxref WHERE accession='source_marker')),
  ((SELECT cv_id FROM cv WHERE name='local'),
   'Repeat Motif', 'The repeat motif, described in PROSITE syntax', 
   (SELECT dbxref_id FROM dbxref WHERE accession='repeat_motif')),
  ((SELECT cv_id FROM cv WHERE name='local'),
   'Restriction Enzyme', 'Restriction Enzyme used to create marker', 
   (SELECT dbxref_id FROM dbxref WHERE accession='restriction_enzyme')),
  ((SELECT cv_id FROM cv WHERE name='local'),
   'Product Length', '', 
   (SELECT dbxref_id FROM dbxref WHERE accession='product_length')),
  ((SELECT cv_id FROM cv WHERE name='local'),
   'Max Length', '', 
   (SELECT dbxref_id FROM dbxref WHERE accession='max_length')),
  ((SELECT cv_id FROM cv WHERE name='local'),
   'Min Length', '', 
   (SELECT dbxref_id FROM dbxref WHERE accession='min_length')),
  ((SELECT cv_id FROM cv WHERE name='local'),
   'PCR Condition', '', 
   (SELECT dbxref_id FROM dbxref WHERE accession='PCR_condition')),
  ((SELECT cv_id FROM cv WHERE name='local'),
   'Sequence Name', 'Name of the marker sequence, if different than marker name or accession', 
   (SELECT dbxref_id FROM dbxref WHERE accession='sequence_name')),
  ((SELECT cv_id FROM cv WHERE name='local'),
   'SNP Alleles', 'SNP alleles in the form A/T', 
   (SELECT dbxref_id FROM dbxref WHERE accession='SNP_alleles')),
  ((SELECT cv_id FROM cv WHERE name='local'),
   'SNP 5-prime Flanking Sequence', 'Upstream flanking sequence for SNP.', 
   (SELECT dbxref_id FROM dbxref WHERE accession='SNP_five_prime_flanking_sequence')),
  ((SELECT cv_id FROM cv WHERE name='local'),
   'SNP 3-prime Flanking Sequence', 'Downstream flanking sequence for SNP', 
   (SELECT dbxref_id FROM dbxref WHERE accession='SNP_three_prime_flanking_sequence')),

  ((SELECT cv_id FROM cv WHERE name='local'),
   'Canonical Marker', 'Official marker (determined by curator)', 
   (SELECT dbxref_id FROM dbxref WHERE accession='canonical_marker')),
  ((SELECT cv_id FROM cv WHERE name='local'),
   'Browser Track Name', 'Official marker (determined by curator)', 
   (SELECT dbxref_id FROM dbxref WHERE accession='browser_track_name'))
;


-- Add to feature_property
INSERT INTO chado.dbxref
  (db_id, accession)
VALUES
  ((SELECT db_id FROM db WHERE name='tripal'), 'experiment_trait_name'),
  ((SELECT db_id FROM db WHERE name='tripal'), 'experiment_trait_description'),
  ((SELECT db_id FROM db WHERE name='tripal'), 'trait_unit'),
  ((SELECT db_id FROM db WHERE name='tripal'), 'qtl_identifier'),
  ((SELECT db_id FROM db WHERE name='tripal'), 'qtl_study_treatment'),
  ((SELECT db_id FROM db WHERE name='tripal'), 'qtl_analysis_method'),
  ((SELECT db_id FROM db WHERE name='tripal'), 'publication_linkage_group'),
  ((SELECT db_id FROM db WHERE name='tripal'), 'interval_calculation_method'),
  ((SELECT db_id FROM db WHERE name='tripal'), 'assigned_linkage_group'),
  ((SELECT db_id FROM db WHERE name='tripal'), 'qtl_peak'),
  ((SELECT db_id FROM db WHERE name='tripal'), 'marker_type'),
  ((SELECT db_id FROM db WHERE name='tripal'), 'sequence_type')
;

INSERT INTO chado.cvterm
  (cv_id, name, definition, dbxref_id)
VALUES
  ((SELECT cv_id FROM cv WHERE name='feature_property'),
   'Experiment Trait Name', 'Trait name used in a specific experiment', 
   (SELECT dbxref_id FROM dbxref WHERE accession='experiment_trait_name')),
   
  ((SELECT cv_id FROM cv WHERE name='feature_property'),
   'Experiment Trait Description', 'Description of trait used in a specific experiment', 
   (SELECT dbxref_id FROM dbxref WHERE accession='experiment_trait_description')),
   
  ((SELECT cv_id FROM cv WHERE name='feature_property'),
   'Trait Unit', 'Unit of measure applied to a trait', 
   (SELECT dbxref_id FROM dbxref WHERE accession='trait_unit')),
   
  ((SELECT cv_id FROM cv WHERE name='feature_property'),
   'QTL Identifier', 'Internal identifier for a QTL', 
   (SELECT dbxref_id FROM dbxref WHERE accession='qtl_identifier')),
   
  ((SELECT cv_id FROM cv WHERE name='feature_property'),
   'QTL Study Treatment', 'Treatment during QTL study', 
   (SELECT dbxref_id FROM dbxref WHERE accession='qtl_study_treatment')),
   
  ((SELECT cv_id FROM cv WHERE name='feature_property'),
   'QTL Analysis Method', 'Method used to determine QTLs', 
   (SELECT dbxref_id FROM dbxref WHERE accession='qtl_analysis_method')),
   
  ((SELECT cv_id FROM cv WHERE name='feature_property'),
   'Publication Linkage Group', 'Linkage group name used in publication', 
   (SELECT dbxref_id FROM dbxref WHERE accession='publication_linkage_group')),
   
  ((SELECT cv_id FROM cv WHERE name='feature_property'),
   'Interval Calculation Method', 'Method used to calculate QTL interval', 
   (SELECT dbxref_id FROM dbxref WHERE accession='interval_calculation_method')),
   
  ((SELECT cv_id FROM cv WHERE name='feature_property'),
   'Assigned Linkage Group', 'Linkage group name assigned to a linkage group map (not its full formal name of map name + linkage group)', 
   (SELECT dbxref_id FROM dbxref WHERE accession='assigned_linkage_group')),
   
  ((SELECT cv_id FROM cv WHERE name='feature_property'),
   'QTL Peak', 'Position of QTL peak', 
   (SELECT dbxref_id FROM dbxref WHERE accession='qtl_peak')),
   
  ((SELECT cv_id FROM cv WHERE name='feature_property'),
   'Marker Type', 'Type of marker, e.g. SSR, STS, CAPS', 
   (SELECT dbxref_id FROM dbxref WHERE accession='marker_type')),
  ((SELECT cv_id FROM cv WHERE name='feature_property'),
   'Primer Pair Name', 'Name of the primer pair used to for a marker.', 
   (SELECT dbxref_id FROM dbxref WHERE accession='qtl_peak')),

  ((SELECT cv_id FROM cv WHERE name='feature_property'),
   'Sequence Type', 'Type of sequence, e.g. SNP', 
   (SELECT dbxref_id FROM dbxref WHERE accession='sequence_type'))
;


-- Add to feature_relationship
INSERT INTO chado.dbxref
  (db_id, accession)
VALUES
  ((SELECT db_id FROM db WHERE name='tripal'), 'nearest_marker'),
  ((SELECT db_id FROM db WHERE name='tripal'), 'flanking_marker_low'),
  ((SELECT db_id FROM db WHERE name='tripal'), 'flanking_marker_high')
;

INSERT INTO chado.cvterm
  (cv_id, name, definition, dbxref_id)
VALUES
  ((SELECT cv_id FROM cv WHERE name='feature_relationship'),
   'Nearest Marker', 'Marker reported to be most closely related to a QTL', 
   (SELECT dbxref_id FROM dbxref WHERE accession='nearest_marker')),

  ((SELECT cv_id FROM cv WHERE name='feature_relationship'),
   'Flanking Marker Low', 'Low flanker marker reported for a QTL', 
   (SELECT dbxref_id FROM dbxref WHERE accession='flanking_marker_low')),

  ((SELECT cv_id FROM cv WHERE name='feature_relationship'),
   'Flanking Marker High', 'High flanker marker reported for a QTL', 
   (SELECT dbxref_id FROM dbxref WHERE accession='flanking_marker_high'))

;


-- Add to featuremap_property
INSERT INTO chado.dbxref
  (db_id, accession)
VALUES
  ((SELECT db_id FROM db WHERE name='tripal'), 'display_map_name'),
  ((SELECT db_id FROM db WHERE name='tripal'), 'publication_map_name'),
  ((SELECT db_id FROM db WHERE name='tripal'), 'featuremap_comment')
;

INSERT INTO chado.cvterm
  (cv_id, name, definition, dbxref_id)
VALUES
  ((SELECT cv_id FROM cv WHERE name='featuremap_property'),
   'Display Map Name', 'The human-readable map name.', 
   (SELECT dbxref_id FROM dbxref WHERE accession='display_map_name')),
   
  ((SELECT cv_id FROM cv WHERE name='featuremap_property'),
   'Publication Map Name', 'Map name used by the publication.', 
   (SELECT dbxref_id FROM dbxref WHERE accession='publication_map_name')),
   
  ((SELECT cv_id FROM cv WHERE name='featuremap_property'),
   'Featuremap Comment', 'Comment on a featuremap record.', 
   (SELECT dbxref_id FROM dbxref WHERE accession='featuremap_comment'))
   
;


-- Add to featuremap_units
INSERT INTO chado.dbxref
  (db_id, accession)
VALUES
  ((SELECT db_id FROM db WHERE name='tripal'), 'cm')
;

INSERT INTO chado.cvterm
  (cv_id, name, definition, dbxref_id)
VALUES
  ((SELECT cv_id FROM cv WHERE name='featuremap_units'),
   'cM', 'centiMorgans', 
   (SELECT dbxref_id FROM dbxref 
    WHERE accession='cm' 
          AND db_id=(SELECT db_id FROM db WHERE name='tripal')))
   
;


-- Add to nd_experiment_types
INSERT INTO chado.dbxref
  (db_id, accession)
VALUES
  ((SELECT db_id FROM db WHERE name='tripal'), 'qtl_experiment')
;

INSERT INTO chado.cvterm
  (cv_id, name, definition, dbxref_id)
VALUES
  ((SELECT cv_id FROM cv WHERE name='nd_experiment_types'),
   'QTL Experiment', 'Indicates an experiment object from a QTL study', 
   (SELECT dbxref_id FROM dbxref 
    WHERE accession='qtl_experiment'
          AND db_id=(SELECT db_id FROM db WHERE name='tripal')))
   
;


-- Add to project_property
INSERT INTO chado.dbxref
  (db_id, accession)
VALUES
  ((SELECT db_id FROM db WHERE name='tripal'), 'project_comment'),
  ((SELECT db_id FROM db WHERE name='tripal'), 'project_map_collection'),
  ((SELECT db_id FROM db WHERE name='tripal'), 'project_type')
;

INSERT INTO chado.cvterm
  (cv_id, name, definition, dbxref_id)
VALUES
  ((SELECT cv_id FROM cv WHERE name='project_property'),
   'Project Comment', 'A project comment', 
   (SELECT dbxref_id FROM dbxref WHERE accession='project_comment')),
   
  ((SELECT cv_id FROM cv WHERE name='project_property'),
   'Project Map Collection', 'A collection of linkage group maps associated with a project', 
   (SELECT dbxref_id FROM dbxref WHERE accession='project_map_collection')),
   
  ((SELECT cv_id FROM cv WHERE name='project_property'),
   'Project Type', 'The type of a project, e.g. "genome assembly", "QTL study"', 
   (SELECT dbxref_id FROM dbxref WHERE accession='project_type'))
   
;


-- Add to pub_type
INSERT INTO chado.dbxref
  (db_id, accession)
VALUES
  ((SELECT db_id FROM db WHERE name='tripal'), 'journal')
;

INSERT INTO chado.cvterm
  (cv_id, name, definition, dbxref_id)
VALUES
  ((SELECT cv_id FROM cv WHERE name='pub_type'),
   'Journal', '', 
   (SELECT dbxref_id FROM dbxref WHERE accession='journal'))
   
;


-- Add to stock_relationship
INSERT INTO chado.dbxref
  (db_id, accession)
VALUES
  ((SELECT db_id FROM db WHERE name='tripal'), 'parent1'),
  ((SELECT db_id FROM db WHERE name='tripal'), 'parent2')
;

INSERT INTO chado.cvterm
  (cv_id, name, definition, dbxref_id)
VALUES
  ((SELECT cv_id FROM cv WHERE name='stock_relationship'),
   'Parent1', 'A parent stock, can be paternal or maternal', 
   (SELECT dbxref_id FROM dbxref 
    WHERE accession='parent1'
          AND db_id=(SELECT db_id FROM db WHERE name='tripal'))),
   
  ((SELECT cv_id FROM cv WHERE name='stock_relationship'),
   'Parent2', 'A parent stock, can be paternal or maternal', 
   (SELECT dbxref_id FROM dbxref 
    WHERE accession='parent2'
          AND db_id=(SELECT db_id FROM db WHERE name='tripal')))
   
;


-- Add to stock_type
INSERT INTO chado.dbxref
  (db_id, accession)
VALUES
  ((SELECT db_id FROM db WHERE name='tripal'), 'mapping_population'),
  ((SELECT db_id FROM db WHERE name='tripal'), 'cultivar')
;

INSERT INTO chado.cvterm
  (cv_id, name, definition, dbxref_id)
VALUES
  ((SELECT cv_id FROM cv WHERE name='stock_type'),
   'Mapping Population', 'Stock record describes a mapping population', 
   (SELECT dbxref_id FROM dbxref 
    WHERE accession='mapping_population'
          AND db_id=(SELECT db_id FROM db WHERE name='tripal'))),
   
  ((SELECT cv_id FROM cv WHERE name='stock_type'),
   'Cultivar', 'Stock record describes a specific Cultivar', 
   (SELECT dbxref_id FROM dbxref 
    WHERE accession='cultivar'
          AND db_id=(SELECT db_id FROM db WHERE name='tripal')))
   
;


-- Add to synonym_type
INSERT INTO chado.dbxref
  (db_id, accession)
VALUES
  ((SELECT db_id FROM db WHERE name='tripal'), 'symbol'),
  ((SELECT db_id FROM db WHERE name='tripal'), 'marker_synonym')
;

INSERT INTO chado.cvterm
  (cv_id, name, definition, dbxref_id)
VALUES
  ((SELECT cv_id FROM cv WHERE name='synonym_type'),
   'Symbol', 'QTL symbol synonym', 
   (SELECT dbxref_id FROM dbxref 
    WHERE accession='symbol'
          AND db_id = (SELECT db_id FROM db 
                       WHERE name='tripal'))),
   
  ((SELECT cv_id FROM cv WHERE name='synonym_type'),
   'Marker Synonym', 'QTL symbol synonym', 
   (SELECT dbxref_id FROM dbxref 
    WHERE accession='marker_synonym'
          AND db_id = (SELECT db_id FROM db 
                       WHERE name='tripal')))
;


-- Add to tripal_pub --
INSERT INTO chado.dbxref
  (db_id, accession)
VALUES
  ((SELECT db_id FROM db WHERE name='tripal'), 'publication_species'),
  ((SELECT db_id FROM db WHERE name='tripal'), 'unpublished_dataset')
;

INSERT INTO chado.cvterm
  (cv_id, name, definition, dbxref_id)
VALUES
  ((SELECT cv_id FROM cv WHERE name='tripal_pub'),
   'Publication Species', '', 
   (SELECT dbxref_id FROM dbxref WHERE accession='publication_species')),
  ((SELECT cv_id FROM cv WHERE name='tripal_pub'),
   'Unpublished Dataset', '', 
   (SELECT dbxref_id FROM dbxref WHERE accession='unpublished_dataset'))
   
;


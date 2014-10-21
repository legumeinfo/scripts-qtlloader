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
  ('genbank:nuccore', 
   'GenBank nucleotide collection',
   'http://www.ncbi.nlm.nih.gov',
   'http://www.ncbi.nlm.nih.gov/nuccore/'),
  ('genbank:taxonomy', 
   'GenBank taxonomy',
   'http://www.ncbi.nlm.nih.gov',
   'http://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id='),
  ('uniprot:species',
   'Uniprot species mnemonics',
   'http://www.uniprot.org/docs/speclist',
   '')
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
  ((SELECT db_id FROM db WHERE name='internal'), 'has_obo_term')
  
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
   (SELECT dbxref_id FROM dbxref WHERE accession='has_obo_term'))

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
  ((SELECT db_id FROM db WHERE name='tripal'), 'assigned_linkage_group')
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
   (SELECT dbxref_id FROM dbxref WHERE accession='assigned_linkage_group'))
   
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
  ((SELECT db_id FROM db WHERE name='tripal'), 'symbol')
;

INSERT INTO chado.cvterm
  (cv_id, name, definition, dbxref_id)
VALUES
  ((SELECT cv_id FROM cv WHERE name='synonym_type'),
   'Symbol', '', 
   (SELECT dbxref_id FROM dbxref 
    WHERE accession='symbol'
          AND db_id = (SELECT db_id FROM db 
                       WHERE name='tripal')))
   
;


-- Add to tripal_pub --
INSERT INTO chado.dbxref
  (db_id, accession)
VALUES
  ((SELECT db_id FROM db WHERE name='tripal'), 'publication_species')
;

INSERT INTO chado.cvterm
  (cv_id, name, definition, dbxref_id)
VALUES
  ((SELECT cv_id FROM cv WHERE name='tripal_pub'),
   'Publication Species', '', 
   (SELECT dbxref_id FROM dbxref WHERE accession='publication_species'))
   
;


-- This file contains SQL needed to update the database to handle script changes.
--
-- New SQL is also added to QTL_cvterms.sql so that file remains the master 
-- copy of SQL statements.


---------------------
----- 03/24/15 ------
---------------------

-- Add to feature_property
INSERT INTO chado.dbxref
  (db_id, accession)
VALUES
  ((SELECT db_id FROM db WHERE name='tripal'), 'sequence_type')
;

INSERT INTO chado.cvterm
  (cv_id, name, definition, dbxref_id)
VALUES
  ((SELECT cv_id FROM cv WHERE name='feature_property'),
   'Sequence Type', 'Type of sequence, e.g. SNP', 
   (SELECT dbxref_id FROM dbxref WHERE accession='sequence_type'))
;

-- PeanutBase only: Create a multi-species record for Arachis
INSERT INTO organism
  (abbreviation, genus, species)
VALUES
  ('arachis', 'Arachis', '.spp');

-- Additional Arachis species
INSERT INTO organism
  (abbreviation, genus, species, common_name)
VALUES
  ('araba', 'Arachis', 'batizocoi', 'Arachis batizocoi'),
  ('araca', 'Arachis', 'cardenasii', 'Arachis cardenasii'),
  ('aradi', 'Arachis', 'diogoi', 'Arachis diogoi')
;

-- PeanutBase only: Use UniProt mnemonics for organism abbreviations
UPDATE organism SET
  abbreviation = 'arahy'
WHERE genus='Arachis' AND species='hypogaea';
UPDATE organism SET
  abbreviation = 'aramo'
WHERE genus='Arachis' AND species='monticola';
UPDATE organism SET
  abbreviation = 'arast'
WHERE genus='Arachis' AND species='stenosperma';
UPDATE organism SET
  abbreviation = 'araip'
WHERE genus='Arachis' AND species='ipaensis';
UPDATE organism SET
  abbreviation = 'arama'
WHERE genus='Arachis' AND species='magna';
UPDATE organism SET
  abbreviation = 'aradu'
WHERE genus='Arachis' AND species='duranensis';

INSERT INTO dbxref
  (db_id, accession, description)
VALUES
  ((SELECT db_id FROM db WHERE name='uniprot:species'),
   'arachis',
   'Represents entities attached to two or more Arachis species.'
  ), 
  ((SELECT db_id FROM db WHERE name='uniprot:species'),
   'araip',
   'Arachis ipaensis mnemonic'
  ), 
  ((SELECT db_id FROM db WHERE name='uniprot:species'),
   'aramo',
   'Arachis monticola mnemonic'
  ), 
  ((SELECT db_id FROM db WHERE name='uniprot:species'),
   'arast',
   'Arachis stenosperma mnemonic'
  ), 
  ((SELECT db_id FROM db WHERE name='uniprot:species'),
   'arama',
   'Arachis magna mnemonic'
  ), 
  ((SELECT db_id FROM db WHERE name='uniprot:species'),
   'aradu',
   'Arachis duranensis mnemonic'
  ),
  ((SELECT db_id FROM db WHERE name='uniprot:species'),
   'araba',
   'Arachis batizocoi mnemonic'
  ),
  ((SELECT db_id FROM db WHERE name='uniprot:species'),
   'araca',
   'Arachis cardenasii mnemonic'
  ),
  ((SELECT db_id FROM db WHERE name='uniprot:species'),
   'aradi',
   'Arachis diogoi mnemonic'
  )
;
 
INSERT INTO organism_dbxref
  (organism_id, dbxref_id)
VALUES
  ((SELECT organism_id FROM organism WHERE abbreviation='arachis'),
   (SELECT dbxref_id FROM dbxref WHERE accession='arachis' 
           AND db_id=(SELECT db_id FROM db WHERE name='uniprot:species'))),
  ((SELECT organism_id FROM organism WHERE abbreviation='araip'),
   (SELECT dbxref_id FROM dbxref WHERE accession='araip' 
           AND db_id=(SELECT db_id FROM db WHERE name='uniprot:species'))),
  ((SELECT organism_id FROM organism WHERE abbreviation='aramo'),
   (SELECT dbxref_id FROM dbxref WHERE accession='aramo' 
           AND db_id=(SELECT db_id FROM db WHERE name='uniprot:species'))),
  ((SELECT organism_id FROM organism WHERE abbreviation='arast'),
   (SELECT dbxref_id FROM dbxref WHERE accession='arast' 
           AND db_id=(SELECT db_id FROM db WHERE name='uniprot:species'))),
  ((SELECT organism_id FROM organism WHERE abbreviation='arama'),
   (SELECT dbxref_id FROM dbxref WHERE accession='arama' 
           AND db_id=(SELECT db_id FROM db WHERE name='uniprot:species'))),
  ((SELECT organism_id FROM organism WHERE abbreviation='aradu'),
   (SELECT dbxref_id FROM dbxref WHERE accession='aradu' 
           AND db_id=(SELECT db_id FROM db WHERE name='uniprot:species'))),
  ((SELECT organism_id FROM organism WHERE abbreviation='araba'),
   (SELECT dbxref_id FROM dbxref WHERE accession='araba' 
           AND db_id=(SELECT db_id FROM db WHERE name='uniprot:species'))),
  ((SELECT organism_id FROM organism WHERE abbreviation='araca'),
   (SELECT dbxref_id FROM dbxref WHERE accession='araca' 
           AND db_id=(SELECT db_id FROM db WHERE name='uniprot:species'))),
  ((SELECT organism_id FROM organism WHERE abbreviation='aradi'),
   (SELECT dbxref_id FROM dbxref WHERE accession='aradi' 
           AND db_id=(SELECT db_id FROM db WHERE name='uniprot:species')))
;




-- Create a linker table for linking multiple species to the same stock record
CREATE TABLE stock_organism (
  stock_organism_id serial NOT NULL,
    PRIMARY KEY (stock_organism_id),
  stock_id INT NOT NULL,
    FOREIGN KEY (stock_id) REFERENCES chado.stock (stock_id) ON DELETE CASCADE INITIALLY DEFERRED,
  organism_id INT NOT NULL,
    FOREIGN KEY (organism_id) REFERENCES chado.organism (organism_id) on DELETE CASCADE INITIALLY DEFERRED,
  rank INT NOT NULL DEFAULT 0,
  CONSTRAINT stock_organism_c1 UNIQUE (stock_id, organism_id, rank)  
);
ALTER TABLE stock_organism OWNER TO www;


---------------------
----- 02/26/15 ------
---------------------

-- Add to feature_property
INSERT INTO chado.dbxref
  (db_id, accession)
VALUES
  ((SELECT db_id FROM db WHERE name='tripal'), 'marker_type')
;

INSERT INTO chado.cvterm
  (cv_id, name, definition, dbxref_id)
VALUES
  ((SELECT cv_id FROM cv WHERE name='feature_property'),
   'Marker Type', 'Type of marker, e.g. SSR, STS, CAPS', 
   (SELECT dbxref_id FROM dbxref WHERE accession='marker_type'))
;

-- Add to synonym_type
INSERT INTO chado.dbxref
  (db_id, accession)
VALUES
  ((SELECT db_id FROM db WHERE name='tripal'), 'marker_synonym')
;

INSERT INTO chado.cvterm
  (cv_id, name, definition, dbxref_id)
VALUES
  ((SELECT cv_id FROM cv WHERE name='synonym_type'),
   'Marker Synonym', 'QTL symbol synonym', 
   (SELECT dbxref_id FROM dbxref 
    WHERE accession='marker_synonym'
          AND db_id = (SELECT db_id FROM db 
                       WHERE name='tripal')))
;

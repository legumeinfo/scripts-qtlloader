-- This file contains SQL needed to update the database to handle script changes.
--
-- New SQL is also added to QTL_cvterms.sql so that file remains the master 
-- copy of SQL statements.

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

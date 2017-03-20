SELECT 
       o.abbreviation AS species,
       ms.cmarker AS marker_identifier, 
       p.uniquename AS citation, 
       s.synonyms,
       t.name AS marker_type,
       mt.value AS source_marker_type,
       md.value AS source_description,
       seq.has_seq,
       prmr.primers,
       c.value AS comment
FROM chado.marker_search ms
  INNER JOIN feature f ON f.feature_id=ms.cmarker_id
  INNER JOIN organism o ON o.organism_id=f.organism_id
  -- publication
  LEFT OUTER JOIN feature_pub fp ON fp.feature_id=f.feature_id
  LEFT OUTER JOIN pub p ON p.pub_id=fp.pub_id
  -- marker_type
  LEFT OUTER JOIN
    (SELECT feature_id, t.name FROM feature_cvterm fc
       INNER JOIN cvterm t ON t.cvterm_id=fc.cvterm_id
     WHERE t.cv_id = (SELECT cv_id FROM cv WHERE name='sequence')
    ) t ON t.feature_id=f.feature_id
  -- synonyms
  LEFT OUTER JOIN 
    (SELECT fs.feature_id, ARRAY_TO_STRING(ARRAY_AGG(s.name),',') AS synonyms 
     FROM feature_synonym fs
       INNER JOIN synonym s ON s.synonym_id=fs.synonym_id
     GROUP BY fs.feature_id
    ) s ON s.feature_id=f.feature_id
  --source marker type
  LEFT OUTER JOIN featureprop mt ON mt.feature_id=f.feature_id
  LEFT OUTER JOIN cvterm mtt ON mtt.cvterm_id=mt.type_id
    AND mtt.name='Marker Type'
  -- marker description
  LEFT OUTER JOIN featureprop md ON md.feature_id=f.feature_id
    AND md.type_id=(SELECT cvterm_id FROM cvterm 
                   WHERE name='Source Description' 
                         AND cv_id=(SELECT cv_id FROM cv 
                                    WHERE name='local'))
 -- has sequence
 LEFT OUTER JOIN
   (SELECT feature_id, 'yes' AS has_seq FROM feature
    WHERE residues IS NOT NULL AND residues != '' AND residues != 'NULL'
   )seq ON seq.feature_id=f.feature_id
 -- primers
 LEFT OUTER JOIN 
    (SELECT feature_id, 
            ARRAY_TO_STRING(ARRAY_AGG(name), ',') AS primers
     FROM
     (
     SELECT fr.object_id AS feature_id, p.name, p.residues
     FROM feature_relationship fr
       INNER JOIN feature p ON p.feature_id=fr.subject_id
     WHERE p.type_id=(SELECT cvterm_id FROM cvterm WHERE name='primer')
     ORDER BY p.name
     ) a
     GROUP BY a.feature_id
    ) prmr ON prmr.feature_id=f.feature_id
  -- comment
  LEFT OUTER JOIN featureprop c ON c.feature_id=f.feature_id
    AND c.type_id=(SELECT cvterm_id FROM cvterm 
                   WHERE name='comment' 
                         AND cv_id=(SELECT cv_id FROM cv 
                                    WHERE name='feature_property'))
        AND c.rank=1
ORDER BY f.name
;

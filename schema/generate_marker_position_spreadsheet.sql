SELECT o.abbreviation AS species,
       ms.cmarker AS marker_identifier,
       m.name AS map,
       lg.name AS lg,
       fp.mappos AS position,
       cmap.accession AS cmap_accession,
       c.value AS comment
FROM chado.marker_search ms
  INNER JOIN feature f ON f.feature_id=ms.cmarker_id
  INNER JOIN organism o ON o.organism_id=f.organism_id
  LEFT OUTER JOIN featurepos fp ON fp.feature_id=f.feature_id
  LEFT OUTER JOIN featuremap m ON m.featuremap_id=fp.featuremap_id
  LEFT OUTER JOIN feature lg ON lg.feature_id=fp.map_feature_id
  LEFT OUTER JOIN 
    (SELECT feature_id, accession 
     FROM feature_dbxref fx 
       INNER JOIN dbxref x ON x.dbxref_id=fx.dbxref_id
     WHERE x.db_id=(SELECT db_id FROM db WHERE name='LIS:cmap')
    ) cmap ON cmap.feature_id=f.feature_id
   LEFT OUTER JOIN featureprop c ON c.feature_id=f.feature_id
      AND c.type_id=(SELECT cvterm_id FROM cvterm 
                     WHERE name='comment' 
                           AND cv_id=(SELECT cv_id FROM cv 
                                      WHERE name='feature_property'))
          AND c.rank=3
ORDER BY f.name
;

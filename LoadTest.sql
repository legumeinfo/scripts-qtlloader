-- TRAITS



------------------------------------------------------------------------------
-- PUBLICATIONS --------------------------------------------------------------
------------------------------------------------------------------------------

-- 1 -------------------------------------------------------------------------
select p.pub_id, p.uniquename as publink, c.value as citation, 
       left(p.title, 20) as title, p.pyear, 
       p.series_name, p.pages, t.name as type, doi.accession as DOI,
       pmid.accession as PMID, left(a.value, 20) as authors,
       left(ab.value, 20) as abstract
from pub p
  inner join cvterm t on t.cvterm_id=p.type_id
  
  inner join pubprop a 
    on a.pub_id=p.pub_id
       and a.type_id = (select cvterm_id from cvterm 
                        where name='Authors'
                              and cv_id=(select cv_id from chado.cv 
                                         where name='tripal_pub'))
  
  inner join pubprop c 
    on c.pub_id=p.pub_id
       and c.type_id = (select cvterm_id from cvterm 
                        where name='Citation'
                              and cv_id=(select cv_id from chado.cv 
                                         where name='tripal_pub'))
                                         
  inner join pubprop ab 
    on ab.pub_id=p.pub_id
       and ab.type_id = (select cvterm_id from cvterm 
                         where name='Abstract'
                               and cv_id=(select cv_id from chado.cv 
                                          where name='tripal_pub'))
                                         
  left join (
    select dx.pub_id, d.accession 
      from pub_dbxref dx
        inner join dbxref d on d.dbxref_id=dx.dbxref_id
      where d.db_id = (select db_id from db where name='DOI')) doi
        on doi.pub_id=p.pub_id
  
  left join (
    select pmx.pub_id, pm.accession 
      from pub_dbxref pmx
        inner join dbxref pm on pm.dbxref_id=pmx.dbxref_id
      where pm.db_id = (select db_id from db where name='PMID')) pmid
        on pmid.pub_id=p.pub_id
  
order by publink;


-- 2 -------------------------------------------------------------------------
select left(title, 20), a.surname, a.givennames 
from pub p
  inner join pubauthor a on a.pub_id=p.pub_id
order by p.title, a.rank;




------------------------------------------------------------------------------
-- MAPS -----------------------------------------------------------------------
------------------------------------------------------------------------------

select m.featuremap_id, m.name, left(m.description, 20) as description,
       u.name as unit, dn.value as display_name, pn.value as pub_map_name,
       pop.value as pop_size, pt.value as pop_type, meth.value as methods,
       s.name as stock, sp.species_list, pub.uniquename as citation, xref.accession as dbxref,
       left(cm.value, 20) as comment
from featuremap m
  inner join cvterm u on u.cvterm_id=m.unittype_id
  
  inner join featuremapprop dn 
    on dn.featuremap_id=m.featuremap_id
       and dn.type_id=(select cvterm_id from cvterm
                       where name='Display Map Name'
                             and cv_id=(select cv_id from cv
                                        where name='featuremap_property'))
                                        
  inner join featuremap_pub fp on fp.featuremap_id=m.featuremap_id
  inner join pub on pub.pub_id = fp.pub_id
  
  left join featuremap_stock fs on fs.featuremap_id=m.featuremap_id
  left join stock s on s.stock_id=fs.stock_id
  
  left join featuremapprop pn 
    on pn.featuremap_id=m.featuremap_id
       and pn.type_id=(select cvterm_id from cvterm
                       where name='Publication Map Name'
                             and cv_id=(select cv_id from cv
                                        where name='featuremap_property'))
                                        
  left join featuremapprop pop 
    on pop.featuremap_id=m.featuremap_id
       and pop.type_id=(select cvterm_id from cvterm
                       where name='Population Size'
                             and cv_id=(select cv_id from cv
                                        where name='featuremap_property'))
                                        
  left join featuremapprop pt 
    on pt.featuremap_id=m.featuremap_id
       and pt.type_id=(select cvterm_id from cvterm
                       where name='Population Type'
                             and cv_id=(select cv_id from cv
                                        where name='featuremap_property'))
                                        
  left join featuremapprop meth 
    on meth.featuremap_id=m.featuremap_id
       and meth.type_id=(select cvterm_id from cvterm
                       where name='Methods'
                             and cv_id=(select cv_id from cv
                                        where name='featuremap_property'))
                                        
  left join featuremapprop cm 
    on cm.featuremap_id=m.featuremap_id
       and cm.type_id=(select cvterm_id from cvterm
                       where name='Featuremap Comment'
                             and cv_id=(select cv_id from cv
                                        where name='featuremap_property'))
                                        
  left join featuremap_dbxref fx on fx.featuremap_id=m.featuremap_id
  left join dbxref xref on xref.dbxref_id=fx.dbxref_id

  left outer join
    (SELECT so.stock_id, string_agg(o.abbreviation, ', ') AS species_list
     FROM stock_organism so
       INNER JOIN organism o ON o.organism_id=so.organism_id
     GROUP BY so.stock_id
    ) AS sp ON sp.stock_id=s.stock_id
  
order by m.name;

select lg.feature_id, lg.name, o.abbreviation as organism, 
       start.mappos as start, stop.mappos as stop, xref.accession as xref
from feature lg
  inner join organism o on o.organism_id=lg.organism_id
  
  inner join featurepos start on start.feature_id=lg.feature_id
  inner join featureposprop sp on sp.featurepos_id=start.featurepos_id
       and sp.type_id=(select cvterm_id from cvterm
                       where name='start'
                             and cv_id=(select cv_id from chado.cv 
                                        where name='featurepos_property'))
                                           
  inner join featurepos stop on stop.feature_id=lg.feature_id
  inner join featureposprop ep 
    on ep.featurepos_id=stop.featurepos_id
       and ep.type_id=(select cvterm_id from cvterm
                       where name='stop'
                             and cv_id=(select cv_id from chado.cv 
                                        where name='featurepos_property'))
                                           
  left join feature_dbxref fx on fx.feature_id=lg.feature_id
  left join dbxref xref on xref.dbxref_id=fx.dbxref_id

where lg.type_id=(select cvterm_id from cvterm
                  where name='linkage_group'
                        and cv_id=(select cv_id from cv
                                   where name='sequence'))
order by lg.name;


------------------------------------------------------------------------------
-- MARKERS -------------------------------------------------------------------
------------------------------------------------------------------------------



select mk.name, o.genus || ' ' || o.species as species, pdbxref.accession as genbank,
       alt.name as alt_name, mt.value as marker_type, lg.name as lg, p.mappos as pos 
from feature mk
  inner join organism o on o.organism_id=mk.organism_id
  left outer join dbxref pdbxref on pdbxref.dbxref_id=mk.dbxref_id
  left outer join feature_synonym fs on fs.feature_id=mk.feature_id
  left outer join synonym alt on alt.synonym_id=fs.synonym_id
  left outer join featureprop mt 
    on mt.feature_id=mk.feature_id 
       and mt.type_id=(select cvterm_id from cvterm 
                       where name='Marker Type' 
                             and cv_id=(select cv_id from cv where name='feature_property'))
  left outer join featurepos p on p.feature_id=mk.feature_id
  left outer join feature lg on lg.feature_id=p.map_feature_id
where mk.type_id = (select cvterm_id from cvterm where name='genetic_marker')
order by mk.name;


select mk.name, mt.value as marker_type 
from feature mk
  inner join featureprop mt 
    on mt.feature_id=mk.feature_id 
       and mt.type_id=(select cvterm_id from cvterm 
                       where name='Marker Type' 
                             and cv_id=(select cv_id from cv where name='feature_property'))
where mk.type_id = (select cvterm_id from cvterm where name='genetic_marker')
;


                                          
------------------------------------------------------------------------------
-- EXPERIMENTS ----------------------------------------------------------------
------------------------------------------------------------------------------
select p.project_id, p.name, left(p.description, 20) as title, 
       left(d.value, 20) as description, 
       left(g.description, 20) as geolocation,
       pub.uniquename as citation, m.value as map_name,
       left(c.value, 20) as comment
from project p
  inner join projectprop d 
    on d.project_id=p.project_id
       and d.type_id = (select cvterm_id from cvterm  
                        where name='Project Description'
                              and cv_id=(select cv_id from cv 
                                         where name='project_property'))
  
  inner join projectprop m
    on m.project_id=p.project_id
       and m.type_id=(select cvterm_id from cvterm
                      where name='Project Map Collection'
                            and cv_id = (select cv_id FROM cv 
                                         where name='project_property'))
  
  inner join projectprop c
    on c.project_id=p.project_id
       and c.type_id=(select cvterm_id from cvterm
                      where name='Project Comment'
                            and cv_id = (select cv_id FROM cv 
                                         where name='project_property'))
  
  inner join nd_experiment_project ep on ep.project_id=p.project_id
  inner join nd_experiment e on e.nd_experiment_id=ep.nd_experiment_id
  inner join nd_geolocation g on g.nd_geolocation_id=e.nd_geolocation_id
  
  inner join project_pub pp on pp.project_id=p.project_id
  inner join pub on pub.pub_id=pp.pub_id
  
order by name;


------------------------------------------------------------------------------
-- QTLs ----------------------------------------------------------------------
------------------------------------------------------------------------------

-- QTL info ------------------------------------------------------------------

SELECT 
  q.feature_id AS qtl_id,
  cf.nid AS qtl_nid,
  q.name AS qtl_name,
  pq.name AS expt_qtl_symbol,
  o.genus || ' ' || o.species AS organism,
  o.common_name,
  co.nid AS organism_nid,
  m.accession AS mnemonic,
  p.uniquename AS citation,
  cp.pub_id AS pub_nid,
  etn.value AS expt_trait_name,
  etd.value AS expt_trait_description,
  tu.value AS trait_unit,
  tc.trait_class,
  tc.qtl_symbol,
  tn.trait_name,
  tc.trait_description,
  s.name AS favorable_allele_source,
  cs.nid AS fas_nid,
  tmt.value AS treatment,
  meth.value AS method,
  lod.rawscore AS lod,
  lr.rawscore AS likelihood_ratio, 
  mr2.rawscore AS marker_r2, 
  tr2.rawscore AS total_r2, 
  add.rawscore AS additivity,
  nm.name AS nearest_marker, 
  nm.feature_id AS nearest_marker_id, 
  fml.name AS flanking_marker_low,
  fml.feature_id AS flanking_marker_low_id,
  fmh.name AS flanking_marker_high,
  fmh.feature_id AS flanking_marker_high_id,
  comm.value AS comment

FROM feature q

  -- get nid for QTL feature
  INNER JOIN public.chado_feature cf ON cf.feature_id = q.feature_id

  INNER JOIN organism o ON o.organism_id=q.organism_id
  INNER JOIN public.chado_organism co ON co.organism_id = o.organism_id
  INNER JOIN organism_dbxref mx ON mx.organism_id=o.organism_id
  INNER JOIN dbxref m ON m.dbxref_id=mx.dbxref_id 
       AND m.db_id=(SELECT db_id FROM db WHERE name='uniprot:species')
       
  -- experiment and publication
  INNER JOIN feature_project fp ON fp.feature_id=q.feature_id
  INNER JOIN project e ON e.project_id=fp.project_id
  INNER JOIN project_pub pp ON pp.project_id=e.project_id
  INNER JOIN pub p ON p.pub_id=pp.pub_id
  INNER JOIN public.chado_pub cp ON cp.pub_id=p.pub_id
    
  -- QTL name used in publication
  LEFT JOIN feature_synonym pqs ON pqs.feature_id=q.feature_id
  LEFT JOIN synonym pq ON pq.synonym_id=pqs.synonym_id

  -- experiment trait name
  INNER JOIN featureprop etn ON etn.feature_id = q.feature_id
    AND etn.type_id=(SELECT cvterm_id FROM chado.cvterm 
                     WHERE name='Experiment Trait Name'
                           AND cv_id=(SELECT cv_id FROM chado.cv 
                                      WHERE name='feature_property'))
                         
  -- experiment trait description
  LEFT JOIN featureprop etd ON etd.feature_id = q.feature_id
    AND etd.type_id=(SELECT cvterm_id FROM chado.cvterm 
                            WHERE name='Experiment Trait Description'
                                  AND cv_id=(SELECT cv_id FROM chado.cv 
                                             WHERE name='feature_property'))
                             
  LEFT JOIN featureprop tu 
    on tu.feature_id=q.feature_id
       and tu.type_id=(select cvterm_id from cvterm
                        where name='Trait Unit'
                              and cv_id=(select cv_id from cv
                                         where name='feature_property'))

  -- trait class
  LEFT JOIN (
    SELECT symf.feature_id, sym.name AS qtl_symbol, 
           sym.definition AS trait_description, t.name AS trait_class
    FROM feature q2 
      -- get QTL symbol 
      LEFT JOIN feature_cvterm symf ON symf.feature_id=q2.feature_id 
      LEFT JOIN cvterm sym ON sym.cvterm_id=symf.cvterm_id
      LEFT JOIN feature_cvtermprop symp ON symp.feature_cvterm_id=symf.feature_cvterm_id 
        AND symp.type_id=(SELECT cvterm_id FROM cvterm 
                          WHERE name='QTL Symbol' 
                                AND cv_id=(SELECT cv_id FROM cv WHERE name='local'))
      -- ... and use it to get the trait class
      LEFT JOIN (SELECT subject_id, name FROM cvterm c 
                   INNER JOIN cvterm_relationship tcr ON tcr.object_id=c.cvterm_id
                 WHERE tcr.type_id=(SELECT cvterm_id FROM cvterm 
                                   WHERE name='Has Trait Class' 
                                         AND cv_id=(SELECT cv_id FROM cv WHERE name='local'))
                ) t ON t.subject_id=sym.cvterm_id
  ) tc ON tc.feature_id=q.feature_id
    
  -- trait name
  LEFT JOIN (
    SELECT tnf.feature_id, t.name AS trait_name
    FROM feature q2 
      -- get QTL symbol 
      LEFT JOIN feature_cvterm tnf ON tnf.feature_id=q2.feature_id 
      LEFT JOIN cvterm tn ON tn.cvterm_id=tnf.cvterm_id
      LEFT JOIN feature_cvtermprop tnp ON tnp.feature_cvterm_id=tnf.feature_cvterm_id 
        AND tnp.type_id=(SELECT cvterm_id FROM cvterm 
                          WHERE name='QTL Symbol' 
                                AND cv_id=(SELECT cv_id FROM cv WHERE name='local'))
      -- ... and use it to get the trait name
      LEFT JOIN (SELECT subject_id, name FROM cvterm c 
                   INNER JOIN cvterm_relationship tnr ON tnr.object_id=c.cvterm_id
                 WHERE tnr.type_id=(SELECT cvterm_id FROM cvterm 
                                   WHERE name='Has Trait Name' 
                                         AND cv_id=(SELECT cv_id FROM cv WHERE name='local'))
                ) t ON t.subject_id=tn.cvterm_id
  ) tn ON tn.feature_id=q.feature_id
    
  -- Favorable Allele Source
  LEFT JOIN feature_stock fs 
    ON fs.feature_id=q.feature_id
       AND fs.type_id=(SELECT cvterm_id FROM chado.cvterm 
                       WHERE name='Favorable Allele Source'
                            AND cv_id=(SELECT cv_id FROM chado.cv 
                                       WHERE name='local'))
  LEFT JOIN stock s ON s.stock_id=fs.stock_id
  LEFT JOIN public.chado_stock cs on cs.stock_id=s.stock_id

  -- treatment
  LEFT JOIN featureprop tmt ON tmt.feature_id = q.feature_id
    AND tmt.type_id=(SELECT cvterm_id FROM chado.cvterm 
                     WHERE name='QTL Study Treatment'
                           AND cv_id=(SELECT cv_id FROM chado.cv 
                                      WHERE name='feature_property'))

  -- analysis method
  LEFT JOIN featureprop meth ON meth.feature_id = q.feature_id
    AND meth.type_id=(SELECT cvterm_id FROM chado.cvterm 
                      WHERE name='QTL Analysis Method'
                            AND cv_id=(SELECT cv_id FROM chado.cv 
                                       WHERE name='feature_property'))

  -- LOD
  LEFT JOIN (
    SELECT af.feature_id, af.rawscore
    FROM analysis a 
      INNER JOIN analysisfeature af ON af.analysis_id=a.analysis_id
    WHERE a.name='LOD'
  ) lod ON lod.feature_id=q.feature_id

  -- likelihood ratio
  LEFT JOIN (
    SELECT af.feature_id, af.rawscore FROM analysis a 
      INNER JOIN analysisfeature af ON af.analysis_id=a.analysis_id
    WHERE a.name='likelihood ratio'
  ) lr ON lr.feature_id=q.feature_id
  
  -- marker R2
  LEFT JOIN (
    SELECT af.feature_id, af.rawscore FROM analysis a 
      INNER JOIN analysisfeature af ON af.analysis_id=a.analysis_id
    WHERE a.name='marker R2'
  ) mr2 ON mr2.feature_id=q.feature_id
  
  -- total R2
  LEFT JOIN (
    SELECT af.feature_id, af.rawscore FROM analysis a 
      INNER JOIN analysisfeature af ON af.analysis_id=a.analysis_id
    WHERE a.name='total R2'
  ) tr2 ON tr2.feature_id=q.feature_id
  
  -- additivity
  LEFT JOIN (
    SELECT AF.feature_id, AF.rawscore FROM analysis A 
      INNER JOIN analysisfeature AF ON AF.analysis_id=A.analysis_id
    WHERE A.name='additivity'
  ) add ON add.feature_id=q.feature_id  

  -- nearest marker
  LEFT JOIN feature_relationship nmr
    ON nmr.subject_id=q.feature_id 
      AND nmr.type_id=(SELECT cvterm_id FROM chado.cvterm 
         WHERE name='Nearest Marker' 
           AND cv_id=(SELECT cv_id FROM chado.cv 
                      WHERE name='feature_relationship'))
  LEFT JOIN feature nm ON nm.feature_id=nmr.object_id
  
  -- flanking marker low
  LEFT JOIN feature_relationship fmlr 
    ON fmlr.subject_id=q.feature_id 
       AND fmlr.type_id=(SELECT cvterm_id FROM chado.cvterm 
                         WHERE name='Flanking Marker Low' 
                               AND cv_id=(SELECT cv_id FROM chado.cv 
                                          WHERE name='feature_relationship'))
  LEFT JOIN feature fml ON fml.feature_id=fmlr.object_id

  -- flanking marker high
  LEFT JOIN feature_relationship fmhr ON fmhr.subject_id=q.feature_id 
    AND FMHR.type_id=(SELECT cvterm_id FROM chado.cvterm 
                      WHERE name='Flanking Marker High' 
                            AND cv_id=(SELECT cv_id FROM chado.cv 
                                       WHERE name='feature_relationship'))
  LEFT JOIN feature fmh ON fmh.feature_id=fmhr.object_id
    
  -- comment
  LEFT JOIN featureprop comm ON comm.feature_id = q.feature_id
    AND comm.type_id=(SELECT cvterm_id FROM chado.cvterm 
                     WHERE name='comment'
                           AND cv_id=(SELECT cv_id FROM chado.cv 
                                      WHERE name='feature_property'))

WHERE q.type_id=(SELECT cvterm_id FROM cvterm 
                 WHERE name='QTL'
                       AND cv_id=(SELECT cv_id FROM chado.cv 
                                  WHERE name='sequence'))

-- Map position ------------------------------------------------------------------

SELECT 
  q.feature_id AS qtl_id,
  cf.nid AS qtl_nid,
  q.name AS qtl_symbol,
  m.name AS map_name,
  m.featuremap_id AS map_id,
  lg.value AS lg,
  lgm.feature_id AS lg_id,
  CAST(loc.fmin as float)/100.0 AS start, 
  CAST(loc.fmax as float)/100.0 AS end,
  flp.value AS int_calc_meth,
  mpop.name AS mapping_population,
  mpop.stock_id AS mapping_population_id,
  p1.parent_name AS parent1,
  p1.subject_id AS parent1_id,
  p2.parent_name AS parent2,
  p2.subject_id AS parent2_id,
  lislgacc.accession AS lis_lg_map_accession, 
  lisacc.accession AS lis_map_accession
  
FROM feature q

  -- get nid for QTL feature
  LEFT JOIN public.chado_feature cf ON cf.feature_id = q.feature_id

  INNER JOIN featureloc loc ON loc.feature_id=q.feature_id
  INNER JOIN feature lgm ON lgm.feature_id=loc.srcfeature_id
  INNER JOIN featureprop lg ON lgm.feature_id=lg.feature_id
  
  LEFT JOIN featurelocprop flp 
    ON flp.featureloc_id=loc.featureloc_id
       AND flp.type_id=(SELECT cvterm_id FROM cvterm 
                        WHERE name='Interval Calculation Method'
                              AND cv_id=(SELECT cv_id FROM cv 
                                         WHERE name='feature_property'))
  
  INNER JOIN featurepos mp ON mp.feature_id=lgm.feature_id
  INNER JOIN featureposprop mpp 
    ON mpp.featurepos_id=mp.featurepos_id
       AND mpp.type_id=(SELECT cvterm_id FROM cvterm
                        WHERE name='start'
                          AND cv_id=(SELECT cv_id FROM chado.cv 
                                     WHERE name='featurepos_property'))
  INNER JOIN featuremap m ON m.featuremap_id=mp.featuremap_id
  LEFT JOIN public.chado_featuremap cm ON cm.featuremap_id=m.featuremap_id
                                        
  LEFT JOIN chado.featuremap_stock fs ON fs.featuremap_id=m.featuremap_id
  LEFT JOIN chado.stock mpop ON mpop.stock_id = fs.stock_id
  LEFT JOIN 
    (SELECT pr.object_id, pr.subject_id, nid.nid AS parent_id, p.name AS parent_name 
     FROM chado.stock_relationship pr 
       INNER JOIN chado.stock p ON p.stock_id = pr.subject_id
       INNER JOIN public.chado_stock nid ON nid.stock_id=p.stock_id
     WHERE pr.type_id = (SELECT cvterm_id FROM chado.cvterm 
                         WHERE name='Parent1' 
                               AND cv_id = (SELECT cv_id FROM chado.cv
                                            WHERE name='stock_relationship'))
    ) p1 ON p1.object_id = mpop.stock_id
  LEFT JOIN 
    (SELECT pr.object_id, pr.subject_id, nid.nid AS parent_id, p.name AS parent_name 
     FROM chado.stock_relationship pr 
       INNER JOIN chado.stock p ON p.stock_id = pr.subject_id
       INNER JOIN public.chado_stock nid ON nid.stock_id=p.stock_id
     WHERE pr.type_id = (SELECT cvterm_id FROM chado.cvterm 
                         WHERE name='Parent2' 
                               AND cv_id = (SELECT cv_id FROM chado.cv 
                                            WHERE name='stock_relationship'))
    ) p2 ON p2.object_id = mpop.stock_id

  LEFT JOIN
   (SELECT LISLGACC.accession, LISLGFDBX.feature_id 
    FROM dbxref LISLGACC
      INNER JOIN feature_dbxref LISLGFDBX 
        ON LISLGFDBX.dbxref_id=LISLGACC.dbxref_id
    WHERE LISLGACC.db_id=(SELECT db_id FROM db 
                          WHERE name='LIS:cmap')
   ) lislgacc ON lislgacc.feature_id=lg.feature_id
    
  LEFT JOIN
   (SELECT LISACC.accession, LISFDBX.featuremap_id 
    FROM dbxref LISACC
      INNER JOIN featuremap_dbxref LISFDBX 
        ON LISFDBX.dbxref_id=LISACC.dbxref_id
    WHERE LISACC.db_id=(SELECT db_id FROM db WHERE name='LIS:cmap')
   ) lisacc ON lisacc.featuremap_id=m.featuremap_id


WHERE q.type_id=(SELECT cvterm_id FROM cvterm 
                 WHERE name='QTL'
                       AND cv_id=(SELECT cv_id FROM chado.cv 
                                  WHERE name='sequence'))

;
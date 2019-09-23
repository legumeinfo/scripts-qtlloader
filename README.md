# QTL loading scripts #

**The corresponding Tripal module is legume_qtl**

This repository contains the scripts for loading QTL, map, and/or marker data into Chado. 
Be warned that these scripts have been through many changes over the years. As the process 
has improved, some initial assumptions proved incorrect. For example, scripts should 
not be executed in the order presumed by their numbering. Another example is that frequent 
changes in column headings necessitated abstracting column heading names.

The step-by-step process of loading QTL is given below.

## Directories ##
*EndNoteTest/* - obsolete. The original source of publication data fields. <br>
*obsolete/* - more obsolete files <br>
*schema/* - SQL scripts and data mapping diagrams <br>
*template/* - data templates <br>

## Data mapping diagram ##
The yED diagram `schema/MapsMarkersQTL.graphml` is helpful in navigating SQL. However, keep in mind that not all parts 
of this diagram have been implemented.

## Scripts ##
**dumpSpreadsheet.pl** -
   Dumps a .xlsx formated Excel spread sheet into text files, one for each work sheet. <br>
   Arguments: 1) spreadsheet-to-dump, 2) directory-for-text-files <br> 
   Example: <br>
      `$ perl dumpSpreadsheet.pl data/arahy/AlvesPereira2008_v01srk.xlsx data/arahy/AlvesPereira2008` 
    <br><br>
**0_verifyWorksheets.pl** - 
   Checks the text files dumped from an Excel spread sheet for errors and data integrity. <br>
   *No longer functional!* Individual scripts do this work now. 
   <br><br>
**1_load_publications.pl** -
  Load data from the publication work sheet (text file). <br>
  Arguments: 1) directory-of-text-files <br> 
  Example: <br>
      `$ perl scripts-qtlloader/1_load_publications.pl data/arahy/AlvesPereira2008` 
  <br><br>
**2_load_maps.pl** - load map data, including marker positions, but not QTL positions. <br>
  Arguments: 1) directory-of-text-files <br> 
  Example: <br>
      `$ perl scripts-qtlloader/2_load_maps.pl data/arahy/AlvesPereira2008` 
  <br><br>
  Must Sync in Tripal feature map (Tripal » Chado Modules » Feature Maps) to be able to see it in map search.
  <br><br>
**3_load_markers.pl** - load marker data, including genomic positions, if provided, but NOT map positions. <br>
  Arguments: 1) directory-of-text-files <br> 
  Example: <br>
      `$ perl scripts-qtlloader/3_load_markers.pl data/arahy/AlvesPereira2008` 
  <br>
  ?? Need instruction: What to do for pre-existing markers. Yes/No/All/etc.
  <br><br>
**4_load_qtl_experiments.pl** - Load QTL experiment (roughly corresponds to a treatment) <br>
  Arguments: 1) directory-of-text-files <br> 
  Example: <br>
      `$ perl scripts-qtlloader/4_load_qtl_experiments.pl data/arahy/AlvesPereira2008` 
  <br><br> 
  Sync in Tripal (Tripal » Chado Modules » Projects)
  <br><br>
**5_load_traits.pl** - **OBSOLETE!** Use `legumeinfo/chado_germplasm_loaders/extractTraitDataFromMaster.pl` instead. 
  <br><br>
**6_load_qtls.pl** - Load QTL data, including map positions. <br>
  Example: <br>
      `$ perl scripts-qtlloader/6_load_qtls.pl data/arahy/AlvesPereira2008` <br> 
  Sync in Tripal (Tripal » Chado Modules » Features: Sync with feature type QTL, uppercase; urganism as appropriate; ). 
  Then, populate MViews in Tripal (Tripal » Chado Schema » Materialized Views. Names: qtl and qtl_map_position).
      
**db.pl** - Contains connectToDB() function, which holds the Postres connection string. 

### Other scripts: ###
**createSpreadsheet.pl** - extracted data from Chado to create a spreadsheet for download. **No longer used and 
likely not up to date.** <br>
**findDuplicateMarkers.pl** - QC script to search Chado for duplicate markers. Data curator will need to decide
which markers are in fact duplicates and what to do about them. <br>
**setCanonicaMarkers.pl** - sets 'canonical' markers. Was used for Phaseolus markers only. The utility of 
designating some markers as 'canonical' is uncertain. 

### Script library ### 
**CropLegumeBaseLoaderUtils.pm**

## Process overview ##
**Note:** Any script can be run multiple times without risk of adding multiple records.<br>
1. The data must be in an **.xlsx** formatted Excel spread sheet. See the `template/` directory.
2. Make sure there are no new trait terms. If so, load with `extractTraitDataFromMaster.pl` in the set of scripts in the
legumeinfo/chado_germplams_loaders/ [GitHub repository](https://github.com/legumeinfo/chado_germplasm_loaders).
3. Dump the data into text files, one per worksheet, named by the worksheet name.
4. If there is publication data in the PUB worksheet (text file), load it, then sync 
   the records using the Tripal pub module admin pages.
5. If there is marker data, load the MARKER and MARKER_GENOMIC_POSITION 
   worksheets (text files). Then synchronize feature records of type 'genetic_marker' using 
   the Tripal feature admin pages.
6. If there is map data, load the MAP, MARKER_POSITION, and MAP_COLLECTION worksheets (text files),
   then sync featuremaps and features of type 'linkage_group' using the featuremap
   and feature Tripal module admin pages.
7. If there is QTL data, load the QTL and QTL_EXPERIMENT worksheets (text files), then
   sync features of type 'qtl' using the Tripal feature admin pages.
8. If marker data was loaded, repopulate the MView 'marker_search' using the Tripal Chado Schema
   admin pages.
9. If QTL data was loaded, repopulate the 'qtl' and 'qtl_map_position' MViews using the Tripal 
   Chado Schema admin pages.
   
Additional activities (unrelated to scripts)
1. If any map data, add the map to the Data Store, in cmap format. See 
/data/public/Arachis_duranensis/AA_aradu_x_aradu_a.map.82HL/ as an example.
2. *Future?* In the past the data spreadsheet was provided as a download. Don't know if there would be reason to 
revive this through the Data Store.

## Debugging hints ##
- check each step
- hand-check data loaded using the yED schema diagram `schema/MapsMarkersQTL.graphml`
- hand-check MView populate scripts in `legumeinfo/legume_qtl/scriptlets/`
- miss-constructed MViews usually mean something wasn't sync-ed. This can be checked by searching for data records 
through the tripal interface. If the records show up but aren't linked to a record, they haven't been sync-ed. Also
check `/admin/tripal/schema/mviews` page and/or `/admin/reports/dblog` to see if there are any errors reported.

## An Example ##
Data file: AgarwalClevenger2018_v03eksc.xlsx <br>
  
1. Dump the spreadsheet into a directory, each worksheet in a separate file: <br>
  `$ perl scripts-qtlloader/dumpSpreadsheet.pl data/arahy/AgarwalClevenger2018_v03eksc.xlsx data/arahy/AgarwalClevenger2018`
 
2. <del>Verify that data is correct: <br>
  `$ perl scripts-qtlloader/0_verifyWorksheets.pl -p data/arahy.16_03_01/CucMace2008`<del>
  
3. Load publication(s) listed on PUB worksheet: <br>
  `$ perl scripts-qtlloader/1_load_publications.pl data/arahy/AgarwalClevenger2018` <br>
    Sync pub records with either the Tripal or Drush interfaces.

4. Load markers. (Note that script numbering is out of order on this step) <br>
  `$ perl scripts-qtlloader/3_load_markers.pl data/arahy/AgarwalClevenger2018` <br>
    Sync feature records of type 'genetic_marker' using Tripal or Drush interfaces <br>
    Rebuild 'marker_search' MView using Tripal or Drush interfaces
    
4. Load maps: <br>
  `$ perl scripts-qtlloader/2_load_maps.pl data/arahy/AgarwalClevenger2018` <br>
    Sync feature records of type 'linkage_group' using Tripal or Drush interfaces <br>
    Sync featuremap records using Tripal or Drush interfaces
  
5. Load QTL experiments (an "experiment" roughly corresponds to a treatment: different conditions, location, time period) <br>       
   `$ perl scripts-qtlloader/4_load_qtl_experiments.pl data/arahy/AgarwalClevenger2018`
    Sync project records using Tripal or Drush interfaces.
  
6. Load the QTL data: <br>
  `$ perl scripts-qtlloader/6_load_qtls.pl data/arahy/AgarwalClevenger2018` <br>
    Sync feature records of type 'QTL' using Tripal or Drush interfaces (feature type is case sensitive) <br>
    Repopulate MViews 'qtl' and 'qtl_map_position' using Tripal or Drush interfaces
    
7. Test publication, markers, map, QTL data in website.


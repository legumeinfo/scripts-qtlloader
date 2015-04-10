CREATE TABLE chado.featurelocprop (
    featurelocprop_id serial not null,
      primary key (featurelocprop_id),
    featureloc_id int not null,
      foreign key (featureloc_id) references chado.featureloc (featureloc_id) on delete cascade INITIALLY DEFERRED,
    type_id int not null,
      foreign key (type_id) references chado.cvterm (cvterm_id) on delete cascade INITIALLY DEFERRED,
    value text null,
    rank int not null default 0,
    
    constraint featurelocprop_c1 unique (featureloc_id, type_id, rank)
);
ALTER TABLE featurelocprop OWNER TO www;

-- This table is included in Tripal 1.1 and may already exist.
CREATE TABLE chado.featuremapprop (
    featuremapprop_id serial not null,
      primary key (featuremapprop_id),
    featuremap_id int not null,
      foreign key (featuremap_id) references chado.featuremap (featuremap_id) on delete cascade INITIALLY DEFERRED,
    type_id int not null,
      foreign key (type_id) references chado.cvterm (cvterm_id) on delete cascade INITIALLY DEFERRED,
    value text null,
    rank int not null default 0,
    
    constraint featuremapprop_c1 unique (featuremap_id, type_id, rank)
);
ALTER TABLE featuremapprop OWNER TO www;


-- This table is included in Tripal 1.1 and may already exist.
CREATE TABLE chado.featuremap_dbxref (
    featuremap_dbxref_id serial not null,
      primary key (featuremap_dbxref_id),
    featuremap_id int not null,
      foreign key (featuremap_id) references featuremap (featuremap_id) on delete cascade INITIALLY DEFERRED,
    dbxref_id int not null,
      foreign key (dbxref_id) references dbxref (dbxref_id) on delete cascade INITIALLY DEFERRED,
      
    constraint featuremap_dbxref_c1 unique (featuremap_id,dbxref_id)
);
create index featuremap_dbxref_idx1 on featuremap_dbxref (featuremap_id);
create index featuremap_dbxref_idx2 on featuremap_dbxref (dbxref_id);
ALTER TABLE featuremap_dbxref OWNER TO www;


CREATE TABLE chado.featuremap_stock (
    featuremap_stock_id serial not null,
      primary key (featuremap_stock_id),
    featuremap_id int not null,
      foreign key (featuremap_id) references chado.featuremap (featuremap_id) on delete cascade INITIALLY DEFERRED,
    stock_id int not null,
      foreign key (stock_id) references chado.stock (stock_id)  on delete cascade INITIALLY DEFERRED
);
ALTER TABLE featuremap_stock OWNER TO www;


CREATE TABLE chado.featureposprop (
    featureposprop_id serial not null,
      primary key (featureposprop_id),
    featurepos_id int not null,
      foreign key (featurepos_id) references chado.featurepos (featurepos_id) on delete cascade INITIALLY DEFERRED,
    type_id int not null,
      foreign key (type_id) references chado.cvterm (cvterm_id) on delete cascade INITIALLY DEFERRED,
    value text null,
    rank int not null default 0,
    
    constraint featureposprop_c1 unique (featurepos_id, type_id, rank)
);
ALTER TABLE featureposprop OWNER TO www;

CREATE TABLE chado.feature_project (
  feature_project_id serial NOT NULL,
    PRIMARY KEY (feature_project_id),
  feature_id INT NOT NULL,
    FOREIGN KEY (feature_id) REFERENCES chado.feature (feature_id) on DELETE CASCADE INITIALLY DEFERRED,
  project_id INT NOT NULL,
    FOREIGN KEY (project_id) REFERENCES chado.project (project_id) ON DELETE CASCADE INITIALLY DEFERRED,
  rank INT NOT NULL DEFAULT 0,
  
  CONSTRAINT feature_project_c1 UNIQUE (feature_id, project_id, rank)
); 
ALTER TABLE feature_project OWNER TO www;

CREATE TABLE chado.feature_stock (
  feature_stock_id serial NOT NULL,
    PRIMARY KEY (feature_stock_id),
  feature_id INT NOT NULL,
    FOREIGN KEY (feature_id) REFERENCES chado.feature (feature_id) on DELETE CASCADE INITIALLY DEFERRED,
  stock_id INT NOT NULL,
    FOREIGN KEY (stock_id) REFERENCES chado.stock (stock_id) ON DELETE CASCADE INITIALLY DEFERRED,
  type_id INT NOT NULL,
    FOREIGN KEY (type_id) REFERENCES chado.cvterm (cvterm_id) ON DELETE CASCADE INITIALLY DEFERRED,
  rank INT NOT NULL DEFAULT 0,
  CONSTRAINT feature_stock_c1 UNIQUE (feature_id, stock_id, type_id, rank)  
);
ALTER TABLE feature_stock OWNER TO www;


-- Create a linker table for linking multiple species to the same stock record
CREATE TABLE stock_organism (
  stock_organism_id serial NOT NULL,
    PRIMARY KEY (feature_stock_id),
  stock_id INT NOT NULL,
    FOREIGN KEY (stock_id) REFERENCES chado.stock (stock_id) ON DELETE CASCADE INITIALLY DEFERRED,
  organism_id INT NOT NULL,
    FOREIGN KEY (feature_id) REFERENCES chado.organism (feature_id) on DELETE CASCADE INITIALLY DEFERRED,
  rank INT NOT NULL DEFAULT 0,
  CONSTRAINT stock_organism_c1 UNIQUE (stock_id, organism_id, type_id, rank)  
);
ALTER TABLE feature_stock OWNER TO www;


-- ============================================
-- Database Schema for Opening Dates Pipeline
-- PostgreSQL Implementation
-- ============================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================
-- CORE TABLES
-- ============================================

-- Entities (POIs, Properties, Buildings)
CREATE TABLE entities (
    entity_id BIGSERIAL PRIMARY KEY,
    uprn BIGINT UNIQUE,
    name VARCHAR(500),
    entity_type VARCHAR(50) NOT NULL,  -- 'poi', 'property', 'business', 'building'
    
    -- Location
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    postcode VARCHAR(10),
    address TEXT,
    geom GEOMETRY(Point, 4326),  -- PostGIS geometry
    
    -- Classification
    category VARCHAR(100),
    poi_type VARCHAR(100),
    
    -- Metadata
    description TEXT,
    website_url TEXT,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_enriched_at TIMESTAMP
);

-- Opening dates (multi-source, one row per source per entity)
CREATE TABLE opening_dates (
    opening_date_id BIGSERIAL PRIMARY KEY,
    entity_id BIGINT NOT NULL REFERENCES entities(entity_id) ON DELETE CASCADE,
    
    -- Date information
    opening_date DATE NOT NULL,
    opening_date_year INTEGER,
    opening_date_confidence VARCHAR(20) NOT NULL,  -- 'high', 'medium', 'low'
    
    -- Source information
    source_name VARCHAR(50) NOT NULL,  -- 'wikidata', 'companies_house', 'land_registry', etc.
    source_id VARCHAR(255),  -- External ID (e.g., Wikidata Q-ID, company number)
    source_metadata JSONB,  -- Additional source-specific data
    
    -- Data quality
    is_primary BOOLEAN DEFAULT FALSE,  -- Best date for this entity
    validation_status VARCHAR(20) DEFAULT 'pending',  -- 'validated', 'pending', 'flagged'
    
    -- Audit trail
    extracted_at TIMESTAMP,  -- When date was extracted from source
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When loaded into DB
    
    -- Constraints
    CONSTRAINT unique_entity_source_date UNIQUE (entity_id, source_name, opening_date)
);

-- Data sources registry
CREATE TABLE data_sources (
    source_id SERIAL PRIMARY KEY,
    source_name VARCHAR(50) UNIQUE NOT NULL,
    source_type VARCHAR(50),  -- 'api', 'download', 'scrape'
    source_url TEXT,
    
    -- Coverage
    total_records BIGINT,
    coverage_start_date DATE,
    coverage_end_date DATE,
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    last_updated_at TIMESTAMP,
    update_frequency VARCHAR(50),  -- 'daily', 'weekly', 'monthly', 'real-time'
    
    -- Metadata
    description TEXT,
    auth_required BOOLEAN DEFAULT FALSE,
    cost VARCHAR(20) DEFAULT 'free',
    reliability VARCHAR(20)  -- 'very_high', 'high', 'medium', 'low'
);

-- Data load history (audit trail)
CREATE TABLE data_loads (
    load_id BIGSERIAL PRIMARY KEY,
    source_id INTEGER REFERENCES data_sources(source_id),
    source_name VARCHAR(50) NOT NULL,
    
    -- Load metadata
    load_type VARCHAR(50),  -- 'full', 'incremental', 'update'
    records_loaded BIGINT,
    records_new BIGINT,
    records_updated BIGINT,
    records_failed BIGINT,
    
    -- Status
    status VARCHAR(20),  -- 'success', 'failed', 'partial'
    error_message TEXT,
    
    -- Timestamps
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    duration_seconds INTEGER,
    
    -- File/API info
    source_file_path TEXT,
    source_file_hash VARCHAR(64),  -- For deduplication
    
    -- Metadata
    metadata JSONB
);

-- Entity relationships (for linking related entities)
CREATE TABLE entity_relationships (
    relationship_id BIGSERIAL PRIMARY KEY,
    entity_id_1 BIGINT NOT NULL REFERENCES entities(entity_id),
    entity_id_2 BIGINT NOT NULL REFERENCES entities(entity_id),
    relationship_type VARCHAR(50),  -- 'same_building', 'parent_child', 'alias', 'duplicate'
    confidence DECIMAL(3, 2),  -- 0.00 to 1.00
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT different_entities CHECK (entity_id_1 != entity_id_2)
);

-- Data quality metrics (per entity)
CREATE TABLE data_quality_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    entity_id BIGINT NOT NULL REFERENCES entities(entity_id) ON DELETE CASCADE,
    
    -- Coverage
    has_opening_date BOOLEAN,
    opening_date_count INTEGER,  -- Number of sources with dates
    primary_source VARCHAR(50),
    
    -- Quality scores
    confidence_score DECIMAL(3, 2),  -- Average confidence (0.00 to 1.00)
    data_completeness DECIMAL(3, 2),  -- 0.00 to 1.00
    
    -- Validation
    validation_flags JSONB,  -- Array of validation issues
    last_validated_at TIMESTAMP,
    
    -- Timestamps
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_entity_metric UNIQUE (entity_id)
);

-- ============================================
-- SOURCE-SPECIFIC TABLES (Optional)
-- ============================================

-- Companies House data
CREATE TABLE companies_house_data (
    company_number VARCHAR(20) PRIMARY KEY,
    company_name VARCHAR(500) NOT NULL,
    incorporation_date DATE,
    dissolution_date DATE,
    company_status VARCHAR(50),
    company_type VARCHAR(50),
    registered_address TEXT,
    sic_codes TEXT[],
    officers JSONB,
    accounts JSONB,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Land Registry transactions
CREATE TABLE land_registry_transactions (
    transaction_id BIGSERIAL PRIMARY KEY,
    transaction_unique_id VARCHAR(50) UNIQUE,  -- From Land Registry
    postcode VARCHAR(10),
    address TEXT,
    transaction_date DATE NOT NULL,
    price DECIMAL(12, 2),
    property_type VARCHAR(50),
    new_build BOOLEAN,
    tenure_type VARCHAR(50),
    first_transaction BOOLEAN,  -- Computed: first transaction for this property
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- EPC data
CREATE TABLE epc_data (
    epc_id BIGSERIAL PRIMARY KEY,
    uprn BIGINT,
    postcode VARCHAR(10),
    address TEXT,
    construction_age INTEGER,
    construction_age_band VARCHAR(50),
    energy_rating VARCHAR(10),
    floor_area DECIMAL(10, 2),
    property_type VARCHAR(50),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Planning data
CREATE TABLE planning_data (
    planning_id BIGSERIAL PRIMARY KEY,
    application_id VARCHAR(100) UNIQUE,
    uprn BIGINT,
    postcode VARCHAR(10),
    address TEXT,
    description TEXT,
    application_date DATE,
    decision_date DATE,
    completion_date DATE,
    start_date DATE,
    decision_type VARCHAR(50),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Heritage data
CREATE TABLE heritage_data (
    heritage_id BIGSERIAL PRIMARY KEY,
    list_entry_number VARCHAR(50) UNIQUE,
    uprn BIGINT,
    name VARCHAR(500),
    address TEXT,
    construction_date DATE,
    construction_period VARCHAR(100),
    grade VARCHAR(10),  -- I, II*, II
    description TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- INDEXES
-- ============================================

-- Entities indexes
CREATE INDEX idx_entities_uprn ON entities(uprn);
CREATE INDEX idx_entities_postcode ON entities(postcode);
CREATE INDEX idx_entities_entity_type ON entities(entity_type);
CREATE INDEX idx_entities_name ON entities(name);
CREATE INDEX idx_entities_geom ON entities USING GIST(geom);
CREATE INDEX idx_entities_location ON entities(latitude, longitude);

-- Opening dates indexes
CREATE INDEX idx_opening_dates_entity ON opening_dates(entity_id);
CREATE INDEX idx_opening_dates_date ON opening_dates(opening_date);
CREATE INDEX idx_opening_dates_source ON opening_dates(source_name);
CREATE INDEX idx_opening_dates_primary ON opening_dates(entity_id, is_primary) WHERE is_primary = TRUE;
CREATE INDEX idx_opening_dates_confidence ON opening_dates(opening_date_confidence);
CREATE INDEX idx_opening_dates_year ON opening_dates(opening_date_year);

-- Data sources indexes
CREATE INDEX idx_data_sources_name ON data_sources(source_name);
CREATE INDEX idx_data_sources_active ON data_sources(is_active) WHERE is_active = TRUE;

-- Data loads indexes
CREATE INDEX idx_data_loads_source ON data_loads(source_id);
CREATE INDEX idx_data_loads_completed ON data_loads(completed_at);
CREATE INDEX idx_data_loads_status ON data_loads(status);

-- Entity relationships indexes
CREATE INDEX idx_entity_relationships_1 ON entity_relationships(entity_id_1);
CREATE INDEX idx_entity_relationships_2 ON entity_relationships(entity_id_2);
CREATE INDEX idx_entity_relationships_type ON entity_relationships(relationship_type);

-- Data quality indexes
CREATE INDEX idx_data_quality_entity ON data_quality_metrics(entity_id);
CREATE INDEX idx_data_quality_has_date ON data_quality_metrics(has_opening_date) WHERE has_opening_date = TRUE;
CREATE INDEX idx_data_quality_confidence ON data_quality_metrics(confidence_score);

-- Source-specific indexes
CREATE INDEX idx_companies_house_name ON companies_house_data(company_name);
CREATE INDEX idx_companies_house_incorporation ON companies_house_data(incorporation_date);

CREATE INDEX idx_land_registry_postcode_address ON land_registry_transactions(postcode, address);
CREATE INDEX idx_land_registry_date ON land_registry_transactions(transaction_date);
CREATE INDEX idx_land_registry_first ON land_registry_transactions(first_transaction) WHERE first_transaction = TRUE;

CREATE INDEX idx_epc_uprn ON epc_data(uprn);
CREATE INDEX idx_epc_postcode ON epc_data(postcode);
CREATE INDEX idx_epc_construction_age ON epc_data(construction_age);

CREATE INDEX idx_planning_uprn ON planning_data(uprn);
CREATE INDEX idx_planning_completion ON planning_data(completion_date) WHERE completion_date IS NOT NULL;

CREATE INDEX idx_heritage_uprn ON heritage_data(uprn);
CREATE INDEX idx_heritage_construction ON heritage_data(construction_date) WHERE construction_date IS NOT NULL;

-- ============================================
-- MATERIALIZED VIEWS (for performance)
-- ============================================

-- Primary opening dates summary (denormalized for fast queries)
CREATE MATERIALIZED VIEW poi_opening_dates_summary AS
SELECT 
    e.entity_id,
    e.uprn,
    e.name,
    e.entity_type,
    e.latitude,
    e.longitude,
    e.postcode,
    e.address,
    e.category,
    e.poi_type,
    
    -- Primary opening date
    od_primary.opening_date,
    od_primary.opening_date_year,
    od_primary.opening_date_source,
    od_primary.opening_date_confidence,
    od_primary.source_id AS primary_source_id,
    
    -- All sources (aggregated)
    COUNT(DISTINCT od.opening_date_id) AS source_count,
    ARRAY_AGG(DISTINCT od.source_name ORDER BY od.source_name) AS all_sources,
    MIN(od.opening_date) AS earliest_date,
    MAX(od.opening_date) AS latest_date,
    
    -- Quality metrics
    dqm.confidence_score,
    dqm.data_completeness,
    
    -- Timestamps
    e.created_at,
    e.updated_at,
    MAX(od.loaded_at) AS last_enriched_at
    
FROM entities e
LEFT JOIN opening_dates od_primary ON e.entity_id = od_primary.entity_id AND od_primary.is_primary = TRUE
LEFT JOIN opening_dates od ON e.entity_id = od.entity_id
LEFT JOIN data_quality_metrics dqm ON e.entity_id = dqm.entity_id
GROUP BY 
    e.entity_id, e.uprn, e.name, e.entity_type, e.latitude, e.longitude,
    e.postcode, e.address, e.category, e.poi_type,
    od_primary.opening_date, od_primary.opening_date_year,
    od_primary.opening_date_source, od_primary.opening_date_confidence,
    od_primary.source_id, dqm.confidence_score, dqm.data_completeness,
    e.created_at, e.updated_at;

-- Indexes on materialized view
CREATE INDEX idx_mv_summary_uprn ON poi_opening_dates_summary(uprn);
CREATE INDEX idx_mv_summary_opening_date ON poi_opening_dates_summary(opening_date);
CREATE INDEX idx_mv_summary_source ON poi_opening_dates_summary(opening_date_source);
CREATE INDEX idx_mv_summary_postcode ON poi_opening_dates_summary(postcode);
CREATE INDEX idx_mv_summary_entity_type ON poi_opening_dates_summary(entity_type);

-- ============================================
-- FUNCTIONS & TRIGGERS
-- ============================================

-- Function to update geometry from lat/lon
CREATE OR REPLACE FUNCTION update_entity_geometry()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.latitude IS NOT NULL AND NEW.longitude IS NOT NULL THEN
        NEW.geom = ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to auto-update geometry
CREATE TRIGGER trigger_update_geometry
    BEFORE INSERT OR UPDATE ON entities
    FOR EACH ROW
    EXECUTE FUNCTION update_entity_geometry();

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for entities
CREATE TRIGGER trigger_entities_updated_at
    BEFORE UPDATE ON entities
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

-- Function to set primary opening date (best confidence)
CREATE OR REPLACE FUNCTION set_primary_opening_date()
RETURNS TRIGGER AS $$
BEGIN
    -- When a new opening date is added, check if it should be primary
    IF NOT EXISTS (
        SELECT 1 FROM opening_dates
        WHERE entity_id = NEW.entity_id
        AND is_primary = TRUE
        AND (
            opening_date_confidence > NEW.opening_date_confidence
            OR (opening_date_confidence = NEW.opening_date_confidence AND opening_date < NEW.opening_date)
        )
    ) THEN
        -- Unset other primary dates for this entity
        UPDATE opening_dates
        SET is_primary = FALSE
        WHERE entity_id = NEW.entity_id AND opening_date_id != NEW.opening_date_id;
        
        -- Set this as primary
        NEW.is_primary = TRUE;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for opening dates
CREATE TRIGGER trigger_set_primary_date
    BEFORE INSERT OR UPDATE ON opening_dates
    FOR EACH ROW
    EXECUTE FUNCTION set_primary_opening_date();

-- ============================================
-- INITIAL DATA (Data Sources Registry)
-- ============================================

INSERT INTO data_sources (source_name, source_type, source_url, is_active, update_frequency, reliability, description) VALUES
('wikidata', 'api', 'https://query.wikidata.org/sparql', TRUE, 'real-time', 'high', 'Wikidata SPARQL endpoint for POI opening dates'),
('companies_house', 'api', 'https://developer.company-information.service.gov.uk/', TRUE, 'daily', 'very_high', 'Companies House API for incorporation dates'),
('land_registry', 'download', 'https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads', TRUE, 'monthly', 'very_high', 'Land Registry Price Paid Data'),
('epc', 'api', 'https://epc.opendatacommunities.org', TRUE, 'daily', 'very_high', 'EPC data for construction age'),
('planning', 'api', 'https://www.planning.data.gov.uk', TRUE, 'near_real-time', 'high', 'National Planning Applications'),
('building_age', 'download', 'Ordnance Survey', TRUE, 'quarterly', 'very_high', 'OS/NGD Building Age data'),
('heritage', 'download', 'https://historicengland.org.uk/listing/the-list/', TRUE, 'quarterly', 'very_high', 'Historic England Heritage List');

-- ============================================
-- COMMENTS (Documentation)
-- ============================================

COMMENT ON TABLE entities IS 'Core entity table for POIs, properties, buildings, and businesses';
COMMENT ON TABLE opening_dates IS 'Multi-source opening dates with source tracking and confidence scoring';
COMMENT ON TABLE data_sources IS 'Registry of all data sources used in the pipeline';
COMMENT ON TABLE data_loads IS 'Audit trail of all data loads';
COMMENT ON TABLE entity_relationships IS 'Relationships between entities (duplicates, aliases, etc.)';
COMMENT ON TABLE data_quality_metrics IS 'Data quality metrics per entity';

COMMENT ON COLUMN opening_dates.is_primary IS 'TRUE for the best/highest confidence opening date for each entity';
COMMENT ON COLUMN opening_dates.source_metadata IS 'JSONB field for source-specific data (e.g., Wikidata Q-ID, company details)';
COMMENT ON COLUMN entities.geom IS 'PostGIS geometry column for spatial queries';


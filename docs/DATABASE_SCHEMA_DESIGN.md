# Database Schema Design for Opening Dates Pipeline

## Overview

This document describes the optimal database schema for storing multi-source opening date data from the unified pipeline. The schema is designed for:
- **Multi-source data**: 7+ data sources (Wikidata, Companies House, Land Registry, EPC, Planning, Building Age, Heritage)
- **Data lineage**: Track which source provided which date
- **Confidence scoring**: Store confidence levels for each date
- **Temporal queries**: Support historical and time-series analysis
- **Scalability**: Handle millions of POIs/properties efficiently
- **Flexibility**: Support future data sources

## 🎯 Design Principles

1. **Normalized structure**: Separate entities from relationships
2. **Source tracking**: Every date has a source and confidence
3. **Audit trail**: Track when data was loaded/updated
4. **Flexible schema**: Support multiple date sources per entity
5. **Performance**: Optimized indexes for common queries
6. **Data quality**: Store quality metrics and validation flags

## 📊 Core Schema Design

### Option 1: Normalized Schema (Recommended)

Best for: Production systems, data warehouses, complex queries

```sql
-- ============================================
-- CORE ENTITIES
-- ============================================

-- POIs/Properties (main entity)
CREATE TABLE entities (
    entity_id BIGSERIAL PRIMARY KEY,
    uprn BIGINT UNIQUE,  -- Unique Property Reference Number
    name VARCHAR(500),
    entity_type VARCHAR(50),  -- 'poi', 'property', 'business', 'building'
    
    -- Location
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    postcode VARCHAR(10),
    address TEXT,
    
    -- Metadata
    category VARCHAR(100),
    poi_type VARCHAR(100),
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Indexes
    INDEX idx_uprn (uprn),
    INDEX idx_postcode (postcode),
    INDEX idx_location (latitude, longitude),
    INDEX idx_entity_type (entity_type)
);

-- Opening dates (one row per source per entity)
CREATE TABLE opening_dates (
    opening_date_id BIGSERIAL PRIMARY KEY,
    entity_id BIGINT NOT NULL REFERENCES entities(entity_id) ON DELETE CASCADE,
    
    -- Date information
    opening_date DATE NOT NULL,
    opening_date_year INTEGER,
    opening_date_confidence VARCHAR(20),  -- 'high', 'medium', 'low'
    
    -- Source information
    source_name VARCHAR(50) NOT NULL,  -- 'wikidata', 'companies_house', etc.
    source_id VARCHAR(255),  -- External ID (e.g., Wikidata Q-ID, company number)
    source_metadata JSONB,  -- Additional source-specific data
    
    -- Data quality
    is_primary BOOLEAN DEFAULT FALSE,  -- Best date for this entity
    validation_status VARCHAR(20) DEFAULT 'pending',  -- 'validated', 'pending', 'flagged'
    
    -- Audit trail
    extracted_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Indexes
    INDEX idx_entity_id (entity_id),
    INDEX idx_opening_date (opening_date),
    INDEX idx_source (source_name),
    INDEX idx_primary (is_primary),
    INDEX idx_confidence (opening_date_confidence),
    UNIQUE (entity_id, source_name, opening_date)  -- Prevent duplicates
);

-- Data source metadata (track data loads)
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
    update_frequency VARCHAR(50),  -- 'daily', 'weekly', 'monthly'
    
    -- Metadata
    description TEXT,
    auth_required BOOLEAN DEFAULT FALSE,
    cost VARCHAR(20) DEFAULT 'free'
);

-- Data load history (audit trail)
CREATE TABLE data_loads (
    load_id BIGSERIAL PRIMARY KEY,
    source_id INTEGER REFERENCES data_sources(source_id),
    source_name VARCHAR(50),
    
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
    
    INDEX idx_source_id (source_id),
    INDEX idx_completed_at (completed_at)
);

-- Entity relationships (for linking related entities)
CREATE TABLE entity_relationships (
    relationship_id BIGSERIAL PRIMARY KEY,
    entity_id_1 BIGINT NOT NULL REFERENCES entities(entity_id),
    entity_id_2 BIGINT NOT NULL REFERENCES entities(entity_id),
    relationship_type VARCHAR(50),  -- 'same_building', 'parent_child', 'alias'
    confidence DECIMAL(3, 2),  -- 0.00 to 1.00
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_entity_1 (entity_id_1),
    INDEX idx_entity_2 (entity_id_2),
    INDEX idx_relationship_type (relationship_type)
);

-- Data quality metrics (per entity)
CREATE TABLE data_quality_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    entity_id BIGINT NOT NULL REFERENCES entities(entity_id),
    
    -- Coverage
    has_opening_date BOOLEAN,
    opening_date_count INTEGER,  -- Number of sources with dates
    primary_source VARCHAR(50),
    
    -- Quality scores
    confidence_score DECIMAL(3, 2),  -- Average confidence
    data_completeness DECIMAL(3, 2),  -- 0.00 to 1.00
    
    -- Validation
    validation_flags JSONB,  -- Array of validation issues
    last_validated_at TIMESTAMP,
    
    -- Timestamps
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_entity_id (entity_id),
    INDEX idx_has_date (has_opening_date),
    INDEX idx_confidence (confidence_score)
);
```

### Option 2: Denormalized Schema (Simpler)

Best for: Analytics, reporting, simple queries, smaller datasets

```sql
-- Single table with all data (denormalized)
CREATE TABLE enriched_pois (
    poi_id BIGSERIAL PRIMARY KEY,
    
    -- Entity information
    uprn BIGINT,
    name VARCHAR(500),
    entity_type VARCHAR(50),
    category VARCHAR(100),
    poi_type VARCHAR(100),
    
    -- Location
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    postcode VARCHAR(10),
    address TEXT,
    
    -- Opening date (primary/best)
    opening_date DATE,
    opening_date_year INTEGER,
    opening_date_source VARCHAR(50),
    opening_date_confidence VARCHAR(20),
    
    -- All source dates (JSON array)
    all_opening_dates JSONB,  -- [{"source": "wikidata", "date": "2020-01-01", "confidence": "high"}, ...]
    
    -- Source IDs
    wikidata_id VARCHAR(50),
    company_number VARCHAR(20),
    
    -- Data quality
    data_quality_score DECIMAL(3, 2),
    source_count INTEGER,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_enriched_at TIMESTAMP,
    
    -- Indexes
    INDEX idx_uprn (uprn),
    INDEX idx_postcode (postcode),
    INDEX idx_opening_date (opening_date),
    INDEX idx_source (opening_date_source),
    INDEX idx_location (latitude, longitude),
    INDEX idx_entity_type (entity_type)
);
```

## 🔄 Hybrid Approach (Recommended for Production)

Combines benefits of both: normalized for data integrity, denormalized views for performance.

```sql
-- Use normalized schema (Option 1) as base
-- Create materialized views for common queries

-- Primary opening dates view (denormalized for performance)
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
    
    -- Primary opening date
    od_primary.opening_date,
    od_primary.opening_date_year,
    od_primary.opening_date_source,
    od_primary.opening_date_confidence,
    od_primary.source_id AS primary_source_id,
    
    -- All sources (aggregated)
    COUNT(DISTINCT od.opening_date_id) AS source_count,
    ARRAY_AGG(DISTINCT od.source_name) AS all_sources,
    MIN(od.opening_date) AS earliest_date,
    MAX(od.opening_date) AS latest_date,
    
    -- Quality metrics
    dqm.confidence_score,
    dqm.data_completeness,
    
    -- Timestamps
    e.updated_at,
    MAX(od.loaded_at) AS last_enriched_at
    
FROM entities e
LEFT JOIN opening_dates od_primary ON e.entity_id = od_primary.entity_id AND od_primary.is_primary = TRUE
LEFT JOIN opening_dates od ON e.entity_id = od.entity_id
LEFT JOIN data_quality_metrics dqm ON e.entity_id = dqm.entity_id
GROUP BY 
    e.entity_id, e.uprn, e.name, e.entity_type, e.latitude, e.longitude,
    e.postcode, e.address, od_primary.opening_date, od_primary.opening_date_year,
    od_primary.opening_date_source, od_primary.opening_date_confidence,
    od_primary.source_id, dqm.confidence_score, dqm.data_completeness, e.updated_at;

-- Indexes on materialized view
CREATE INDEX idx_mv_uprn ON poi_opening_dates_summary(uprn);
CREATE INDEX idx_mv_opening_date ON poi_opening_dates_summary(opening_date);
CREATE INDEX idx_mv_source ON poi_opening_dates_summary(opening_date_source);
CREATE INDEX idx_mv_postcode ON poi_opening_dates_summary(postcode);
```

## 📋 Complete Schema with All Tables

```sql
-- ============================================
-- COMPLETE SCHEMA
-- ============================================

-- 1. Core entities
CREATE TABLE entities (...);  -- As above

-- 2. Opening dates (multi-source)
CREATE TABLE opening_dates (...);  -- As above

-- 3. Data sources registry
CREATE TABLE data_sources (...);  -- As above

-- 4. Data load history
CREATE TABLE data_loads (...);  -- As above

-- 5. Entity relationships
CREATE TABLE entity_relationships (...);  -- As above

-- 6. Data quality metrics
CREATE TABLE data_quality_metrics (...);  -- As above

-- 7. Source-specific data (optional, for detailed source data)
CREATE TABLE companies_house_data (
    company_number VARCHAR(20) PRIMARY KEY,
    company_name VARCHAR(500),
    incorporation_date DATE,
    registered_address TEXT,
    company_status VARCHAR(50),
    sic_codes TEXT[],
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_company_name (company_name),
    INDEX idx_incorporation_date (incorporation_date)
);

CREATE TABLE land_registry_transactions (
    transaction_id BIGSERIAL PRIMARY KEY,
    postcode VARCHAR(10),
    address TEXT,
    transaction_date DATE,
    price DECIMAL(12, 2),
    property_type VARCHAR(50),
    new_build BOOLEAN,
    first_transaction BOOLEAN,  -- Computed field
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_postcode_address (postcode, address),
    INDEX idx_transaction_date (transaction_date),
    INDEX idx_first_transaction (first_transaction)
);

CREATE TABLE epc_data (
    epc_id BIGSERIAL PRIMARY KEY,
    uprn BIGINT,
    postcode VARCHAR(10),
    address TEXT,
    construction_age INTEGER,
    construction_age_band VARCHAR(50),
    energy_rating VARCHAR(10),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_uprn (uprn),
    INDEX idx_postcode (postcode),
    INDEX idx_construction_age (construction_age)
);

-- 8. Spatial indexes (PostGIS extension)
CREATE EXTENSION IF NOT EXISTS postgis;

ALTER TABLE entities ADD COLUMN geom GEOMETRY(Point, 4326);
CREATE INDEX idx_geom ON entities USING GIST(geom);

-- Update geometry from lat/lon
UPDATE entities SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);
```

## 🔍 Query Examples

### Get primary opening date for all POIs

```sql
-- Using normalized schema
SELECT 
    e.entity_id,
    e.name,
    e.uprn,
    od.opening_date,
    od.opening_date_source,
    od.opening_date_confidence
FROM entities e
JOIN opening_dates od ON e.entity_id = od.entity_id
WHERE od.is_primary = TRUE
ORDER BY e.name;

-- Using materialized view (faster)
SELECT * FROM poi_opening_dates_summary
WHERE opening_date IS NOT NULL
ORDER BY name;
```

### Get all opening dates for an entity (multi-source)

```sql
SELECT 
    source_name,
    opening_date,
    opening_date_confidence,
    source_id,
    loaded_at
FROM opening_dates
WHERE entity_id = 12345
ORDER BY opening_date_confidence DESC, opening_date;
```

### Find entities by source

```sql
SELECT 
    e.name,
    e.uprn,
    od.opening_date,
    od.opening_date_source
FROM entities e
JOIN opening_dates od ON e.entity_id = od.entity_id
WHERE od.source_name = 'companies_house'
ORDER BY od.opening_date;
```

### Spatial queries (find POIs near location)

```sql
SELECT 
    name,
    opening_date,
    ST_Distance(geom, ST_SetSRID(ST_MakePoint(-0.1276, 51.5074), 4326)) AS distance_meters
FROM entities
WHERE ST_DWithin(
    geom,
    ST_SetSRID(ST_MakePoint(-0.1276, 51.5074), 4326),
    1000  -- 1km radius
)
ORDER BY distance_meters;
```

### Data quality analysis

```sql
SELECT 
    opening_date_source,
    COUNT(*) AS count,
    AVG(CASE 
        WHEN opening_date_confidence = 'high' THEN 1.0
        WHEN opening_date_confidence = 'medium' THEN 0.7
        WHEN opening_date_confidence = 'low' THEN 0.4
        ELSE 0.0
    END) AS avg_confidence_score
FROM opening_dates
WHERE is_primary = TRUE
GROUP BY opening_date_source
ORDER BY count DESC;
```

## 🚀 Implementation Recommendations

### Database Choice

1. **PostgreSQL** (Recommended)
   - Excellent JSONB support for flexible metadata
   - PostGIS extension for spatial queries
   - Strong performance for complex queries
   - ACID compliance

2. **MySQL/MariaDB**
   - Good for simpler schemas
   - Less flexible JSON support
   - Good performance

3. **SQLite**
   - Good for development/testing
   - Not recommended for production (concurrency limits)

### Indexing Strategy

```sql
-- Primary indexes (always)
CREATE INDEX idx_entities_uprn ON entities(uprn);
CREATE INDEX idx_opening_dates_entity ON opening_dates(entity_id);
CREATE INDEX idx_opening_dates_date ON opening_dates(opening_date);
CREATE INDEX idx_opening_dates_source ON opening_dates(source_name);

-- Composite indexes for common queries
CREATE INDEX idx_entities_location ON entities(latitude, longitude);
CREATE INDEX idx_opening_dates_primary ON opening_dates(entity_id, is_primary) WHERE is_primary = TRUE;

-- Full-text search (if needed)
CREATE INDEX idx_entities_name_fts ON entities USING GIN(to_tsvector('english', name));
```

### Partitioning (for large datasets)

```sql
-- Partition opening_dates by year
CREATE TABLE opening_dates_2020 PARTITION OF opening_dates
    FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');

CREATE TABLE opening_dates_2021 PARTITION OF opening_dates
    FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
-- etc.
```

### Data Loading Strategy

1. **Initial Load**: Bulk insert from CSV/Parquet
2. **Incremental Updates**: Track `loaded_at` timestamps
3. **Deduplication**: Use `UNIQUE` constraints
4. **Validation**: Check data quality before insert

## 📊 Schema Comparison

| Feature | Normalized | Denormalized | Hybrid |
|---------|------------|--------------|--------|
| **Data Integrity** | ✅ High | ⚠️ Medium | ✅ High |
| **Query Performance** | ⚠️ Medium | ✅ Fast | ✅ Fast (with views) |
| **Storage** | ✅ Efficient | ⚠️ Larger | ✅ Efficient |
| **Flexibility** | ✅ High | ⚠️ Medium | ✅ High |
| **Complexity** | ⚠️ Higher | ✅ Simple | ⚠️ Medium |
| **Best For** | Production | Analytics | Production |

## 🎯 Recommended Approach

**Use Hybrid Schema:**
1. Normalized base tables (data integrity)
2. Materialized views (query performance)
3. Source-specific tables (detailed data)
4. Spatial indexes (PostGIS)
5. Partitioning (for scale)

This gives you:
- ✅ Data integrity and audit trail
- ✅ Fast queries via materialized views
- ✅ Flexible schema for new sources
- ✅ Spatial query support
- ✅ Scalability

## 📝 Migration Script

See `scripts/create_database_schema.sql` for complete implementation.


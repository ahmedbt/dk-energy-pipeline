-- enriched_capacity.sql
-- Combines capacity data with municipality names and regions
{{ config(materialized='table') }}
SELECT 
    -- Primary identifiers
    c.Month,
    c.MunicipalityNo,
    
    -- Enriched municipality information
    m.municipality_name,
    m.municipality_name_danish,
    m.region_name,
    m.region_name_danish,
    
    -- Capacity metrics
    c.CapacityGe100MW,
    c.CapacityLt100MW,
    c.OffshoreWindCapacity,
    c.OnshoreWindCapacity,
    c.SolarPowerCapacity,
    
    -- Unit counts
    c.NumberGenerationUnitsGe100MW,
    c.NumberGenerationUnitsLt100MW,
    c.NumberOffshoreWindGenerators,
    c.NumberOnshoreWindGenerators,
    c.NumberSolarPanels,
    
    -- Derived metrics (calculated on the fly)
    (c.CapacityGe100MW + c.CapacityLt100MW) AS total_thermal_capacity_mw,
    (c.OffshoreWindCapacity + c.OnshoreWindCapacity + c.SolarPowerCapacity) AS total_renewable_capacity_mw,
    
    -- Renewable percentage (avoid division by zero)
    CASE 
        WHEN (c.CapacityGe100MW + c.CapacityLt100MW + c.OffshoreWindCapacity + c.OnshoreWindCapacity + c.SolarPowerCapacity) > 0
        THEN ROUND(
            (c.OffshoreWindCapacity + c.OnshoreWindCapacity + c.SolarPowerCapacity) * 100.0 / 
            (c.CapacityGe100MW + c.CapacityLt100MW + c.OffshoreWindCapacity + c.OnshoreWindCapacity + c.SolarPowerCapacity), 
            2
        )
        ELSE 0
    END AS renewable_percentage

FROM {{ source('dk_energy', 'capacity_per_municipality') }} c
LEFT JOIN {{ source('dk_energy', 'municipality_region_mapping')}} m
    ON c.MunicipalityNo = m.municipality_code
ORDER BY c.Month DESC, m.region_name, m.municipality_name
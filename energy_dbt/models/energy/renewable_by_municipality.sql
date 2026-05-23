{{ config(materialized='table') }}

SELECT 
  MunicipalityNo,
  MAX(Month) as latest_month,
  SUM(OffshoreWindCapacity + OnshoreWindCapacity + SolarPowerCapacity) as total_renewable_mw,
  SUM(CapacityGe100MW + CapacityLt100MW) as total_thermal_mw,
  ROUND(SAFE_DIVIDE(
    SUM(OffshoreWindCapacity + OnshoreWindCapacity + SolarPowerCapacity),
    SUM(OffshoreWindCapacity + OnshoreWindCapacity + SolarPowerCapacity + CapacityGe100MW + CapacityLt100MW)
  ) * 100, 2) as renewable_percentage
FROM {{ source('dk_energy', 'capacity_per_municipality') }}
GROUP BY MunicipalityNo
ORDER BY renewable_percentage DESC

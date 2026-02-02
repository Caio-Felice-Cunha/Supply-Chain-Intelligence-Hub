# Quick guide for my ETL flow

MySQL Database
    ↓
[1. EXTRACT]
├─ DatabaseConnection (connection pooling, retry logic)
└─ DataExtractor (table extraction, custom queries)
    ↓
[2. TRANSFORM]
├─ Clean nulls (4 strategies)
├─ Remove duplicates
├─ Standardize dates
├─ Add derived columns
└─ Apply business rules
    ↓
[3. VALIDATE]
├─ DataQualityValidator (built-in checks)
├─ DataQualityRulesEngine (custom rules)
├─ DataProfiler (statistics)
└─ AnomalyDetector (outliers)
    ↓
[4. LOAD]
├─ Batch processing
├─ Create backups
├─ Transaction management
└─ Error isolation
    ↓
Clean Database + HTML Reports + Logs

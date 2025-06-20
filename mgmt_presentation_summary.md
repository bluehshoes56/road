# Enhanced Constant Merchant Quarterly Analysis
## Executive Summary for Senior Management

---

## PROCESS OVERVIEW

**Title:** Enhanced Constant Merchant Quarterly Analysis  
**Purpose:** Track and analyze merchant retention, performance, and economic trends using a constant cohort methodology  
**Methodology:** Federal System Banking Institute (FSBI) enhanced SpendTrend approach  
**Key Innovation:** Same-store sales analysis using 14-month constant merchant sample with quarterly tracking

---

## PROCESS FLOW DIAGRAM

```
INPUT DATA
    ↓
PERIOD DEFINITION
• 16-month: Dec 2020 - Mar 2022 (comparison)
• 14-month: Jan 2021 - Feb 2022 (base cohort)
• Quarterly tracking: Mar, Jun, Sep, Dec
    ↓
MERCHANT IDENTIFICATION
• Filter 14-month base period transactions
• Group by merchant_key
• Count distinct active months
• Select merchants present in ALL 14 months
    ↓
BASE ANALYSIS
• Calculate metrics by NAICS3 industry
• Apply enhanced formatting
• Generate monthly trends
• Create performance breakdowns
    ↓
QUARTERLY TRACKING
• Track same merchants beyond Feb 2022
• Measure retention rates
• Analyze performance changes
• Compare against baseline
    ↓
COMPREHENSIVE REPORTING
• Executive dashboards
• Industry analysis
• Retention trends
• Policy insights
```

---

## FOUR KEY PHASES

### Phase 1: Data Preparation & Period Definition
**Description:** Establish analytical framework and time periods

**Key Activities:**
- Define 16-month comparison period (Dec 2020 - Mar 2022)
- Define 14-month base period (Jan 2021 - Feb 2022)
- Identify quarterly tracking points (Mar, Jun, Sep, Dec)
- Validate data completeness and quality

**Deliverables:**
- Time period arrays
- Data validation report
- Quarterly tracking schedule

### Phase 2: Constant Merchant Identification
**Description:** Identify merchants with consistent transaction activity

**Key Activities:**
- Filter transactions for 14-month base period
- Group by merchant_key and count distinct active months
- Select merchants present in ALL 14 months
- Create comparison cohort (16-month period)

**Deliverables:**
- `constant_merchants_14m` flag (primary analysis cohort)
- `constant_merchants_16m` flag (comparison baseline)
- Merchant count summaries by cohort

### Phase 3: Detailed Analysis & Enhanced Formatting
**Description:** Generate comprehensive metrics with professional formatting

**Key Activities:**
- Calculate key performance metrics by NAICS3 industry
- Apply enhanced number formatting (commas, percentages, currency)
- Create monthly trend analysis within base period
- Generate industry-specific performance breakdowns

**Deliverables:**
- NAICS3 industry analysis tables
- Monthly performance trends
- Formatted executive dashboards
- Key metric summaries

### Phase 4: Quarterly Tracking Beyond Base Period
**Description:** Monitor constant merchant performance in future periods

**Key Activities:**
- Track same merchant cohort in quarterly intervals
- Measure retention rates and attrition patterns
- Analyze performance changes over time
- Compare against baseline period metrics

**Deliverables:**
- Quarterly retention reports
- Performance trend analysis
- Attrition pattern insights
- Economic health indicators

---

## KEY PERFORMANCE METRICS

### Merchant Metrics
- Unique merchant count by period
- Merchant retention percentage
- New merchant identification
- Attrition rate calculation

### Transaction Metrics
- Total transaction volume
- Average transaction amount
- Transaction frequency
- Month-over-month growth

### Industry Metrics
- NAICS3 industry breakdown
- Sector-specific performance
- Industry retention patterns
- Cross-industry comparisons

### Economic Indicators
- Same-store sales trends
- Market stability measures
- Economic health proxies
- Policy impact assessment

---

## BUSINESS VALUE PROPOSITION

### Strategic Benefits
- **Same-Store Sales Analysis:** Consistent merchant base eliminates new/closed business noise
- **Market Stability Assessment:** Understand merchant longevity and business sustainability
- **Economic Health Monitoring:** Merchant retention as leading economic indicator
- **Policy Impact Measurement:** Quantify effects of economic policies on small businesses

### Operational Benefits
- **Standardized Reporting:** Consistent methodology across time periods
- **Automated Processing:** Scalable analysis framework for large datasets
- **Quality Assurance:** Enhanced formatting prevents data interpretation errors
- **Executive Dashboards:** Professional presentation-ready outputs

### Analytical Benefits
- **Trend Analysis:** Long-term performance tracking capability
- **Benchmarking:** Historical comparison and industry standards
- **Forecasting:** Predictive insights based on retention patterns
- **National Scaling:** Foundation for Census/QCEW extrapolation

---

## CRITICAL SUCCESS FACTORS

- **Data Quality:** Complete transaction records across all analysis periods
- **Merchant Consistency:** Reliable merchant_key identification across time
- **Industry Classification:** Accurate NAICS3 coding for sector analysis
- **Temporal Alignment:** Consistent month-end cut-offs and reporting periods

---

## NEXT STEPS & ROADMAP

### Immediate
- Validate constant merchant identification logic
- Generate baseline period analysis reports
- Create quarterly tracking schedule

### Short-term
- Implement merchant replacement logic for sample maintenance
- Develop automated quarterly reporting pipeline
- Create executive dashboard visualizations

### Long-term
- Scale analysis to national level using Census/QCEW data
- Integrate with economic forecasting models
- Expand to real-time monitoring capabilities

---

## TECHNICAL IMPLEMENTATION SUMMARY

**Data Source:** gbs_mids_joined (comprehensive merchant transaction dataset)  
**Processing Engine:** Apache Spark (Azure Databricks)  
**Key Functions:** countDistinct(), groupBy(), broadcast(), enhanced formatting  
**Output Formats:** Executive tables, CSV exports, dashboard data, documentation

---

*This analysis provides the foundation for understanding merchant behavior patterns, economic health indicators, and policy impact assessment through rigorous same-store sales methodology.*
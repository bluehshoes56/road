# COMMAND ----------
# Production-Ready Tiered Merchant Replacement with Comprehensive EDA
# Includes before/after analysis by NAICS3 and NAICS6

from pyspark.sql.functions import col, countDistinct, sum as spark_sum, avg, when, lit, broadcast, abs as spark_abs, round as spark_round
from pyspark.sql.window import Window
from datetime import datetime
import pandas as pd

print("Production-Ready Tiered Merchant Replacement with EDA")

# Configuration Parameters
TIER_1_VOLUME_RANGE = (0.3, 1.7)
TIER_2_VOLUME_RANGE = (0.2, 2.0)
TIER_3_VOLUME_RANGE = (0.1, 5.0)
MIN_ACTIVE_MONTHS = 2
RECENT_MONTHS_LOOKBACK = 3

# COMMAND ----------
def calculate_sample_metrics(sample_df, gbs_data, analysis_month, sample_name):
    """
    Calculate comprehensive metrics for a merchant sample
    """
    
    # Filter data for analysis month and join with sample
    month_data = gbs_data.filter(col("txn_year_month") == analysis_month) \
        .join(broadcast(sample_df), on="merchant_key", how="inner")
    
    if month_data.count() == 0:
        print(f"No data found for {sample_name} in month {analysis_month}")
        return None
    
    # NAICS3 Analysis
    naics3_metrics = month_data.groupBy("naics3").agg(
        countDistinct("merchant_key").alias("unique_merchants"),
        spark_sum("adjusted_txn_cnt").alias("total_transactions"),
        spark_sum("total_tran_amount").alias("total_amount"),
        avg("total_tran_amount").alias("avg_amount_per_merchant"),
        spark_round(avg("total_tran_amount") / avg("adjusted_txn_cnt"), 2).alias("avg_amount_per_txn")
    ).withColumn("sample_type", lit(sample_name)) \
     .withColumn("analysis_month", lit(analysis_month)) \
     .withColumn("naics_level", lit("NAICS3"))
    
    # NAICS6 Analysis (if naics6 column exists)
    naics6_metrics = None
    if "naics6" in month_data.columns:
        naics6_metrics = month_data.groupBy("naics6").agg(
            countDistinct("merchant_key").alias("unique_merchants"),
            spark_sum("adjusted_txn_cnt").alias("total_transactions"),
            spark_sum("total_tran_amount").alias("total_amount"),
            avg("total_tran_amount").alias("avg_amount_per_merchant"),
            spark_round(avg("total_tran_amount") / avg("adjusted_txn_cnt"), 2).alias("avg_amount_per_txn")
        ).withColumn("sample_type", lit(sample_name)) \
         .withColumn("analysis_month", lit(analysis_month)) \
         .withColumn("naics_level", lit("NAICS6")) \
         .withColumnRenamed("naics6", "naics_code")
    
    # Overall sample metrics
    overall_metrics = month_data.agg(
        countDistinct("merchant_key").alias("total_merchants"),
        spark_sum("adjusted_txn_cnt").alias("total_transactions"),
        spark_sum("total_tran_amount").alias("total_amount"),
        avg("total_tran_amount").alias("avg_amount_per_merchant"),
        spark_round(spark_sum("total_tran_amount") / spark_sum("adjusted_txn_cnt"), 2).alias("avg_amount_per_txn")
    ).collect()[0]
    
    # Rename naics3 column to match naics6 structure
    naics3_metrics = naics3_metrics.withColumnRenamed("naics3", "naics_code")
    
    return {
        "naics3_metrics": naics3_metrics,
        "naics6_metrics": naics6_metrics,
        "overall_metrics": overall_metrics,
        "sample_name": sample_name,
        "month": analysis_month
    }

# COMMAND ----------
def execute_tiered_replacement_with_eda(current_month, constant_merchants_14m, gbs_mids_joined, target_retention_pct=0.95):
    """
    Execute tiered replacement with comprehensive before/after EDA
    """
    
    print(f"\n=== TIERED REPLACEMENT WITH EDA FOR {current_month} ===")
    
    # Step 1: Calculate BEFORE metrics
    print("Calculating BEFORE replacement metrics...")
    before_metrics = calculate_sample_metrics(
        constant_merchants_14m, 
        gbs_mids_joined, 
        current_month, 
        "Before_Replacement"
    )
    
    # Step 2: Execute replacement logic (same as before)
    previous_month = current_month - 1
    
    prev_active = gbs_mids_joined.filter(col("txn_year_month") == previous_month) \
        .join(broadcast(constant_merchants_14m), on="merchant_key", how="inner") \
        .select("merchant_key").distinct()
    
    curr_active = gbs_mids_joined.filter(col("txn_year_month") == current_month) \
        .join(broadcast(constant_merchants_14m), on="merchant_key", how="inner") \
        .select("merchant_key").distinct()
    
    attrited_merchants = prev_active.join(curr_active, on="merchant_key", how="left_anti")
    attrited_count = attrited_merchants.count()
    
    current_active_count = curr_active.count()
    original_sample_size = constant_merchants_14m.count()
    target_size = int(original_sample_size * target_retention_pct)
    
    print(f"Original sample size: {original_sample_size:,}")
    print(f"Current active merchants: {current_active_count:,}")
    print(f"Attrited this month: {attrited_count:,}")
    print(f"Target size ({target_retention_pct*100}%): {target_size:,}")
    print(f"Replacements needed: {max(0, target_size - current_active_count):,}")
    
    if current_active_count >= target_size:
        print("No replacement needed - sample size within target")
        
        # Even if no replacement, still do EDA comparison
        comparison_results = compare_samples_eda(
            before_metrics, before_metrics, current_month
        )
        
        return {
            "replacement_needed": False,
            "new_sample": curr_active,
            "eda_comparison": comparison_results,
            "before_metrics": before_metrics,
            "after_metrics": before_metrics
        }
    
    # Step 3: Execute replacement logic
    all_merchants = gbs_mids_joined.select("merchant_key").distinct()
    replacement_pool = all_merchants.join(broadcast(constant_merchants_14m), on="merchant_key", how="left_anti")
    
    recent_months = [current_month - i for i in range(1, RECENT_MONTHS_LOOKBACK + 1)]
    
    attrited_profiles = gbs_mids_joined.filter(col("txn_year_month").isin(recent_months)) \
        .join(broadcast(attrited_merchants), on="merchant_key", how="inner") \
        .groupBy("merchant_key", "naics3").agg(
            avg(col("total_tran_amount")).alias("avg_volume"),
            avg(col("adjusted_txn_cnt")).alias("avg_txns"),
            countDistinct("txn_year_month").alias("active_months")
        ).collect()
    
    candidate_profiles = gbs_mids_joined.filter(col("txn_year_month").isin(recent_months)) \
        .join(broadcast(replacement_pool), on="merchant_key", how="inner") \
        .groupBy("merchant_key", "naics3").agg(
            avg(col("total_tran_amount")).alias("avg_volume"),
            avg(col("adjusted_txn_cnt")).alias("avg_txns"),
            countDistinct("txn_year_month").alias("active_months")
        ).filter(col("active_months") >= MIN_ACTIVE_MONTHS) \
        .cache()
    
    # Step 4: Tiered replacement logic
    replacements = []
    tier_stats = {"tier_1": 0, "tier_2": 0, "tier_3": 0, "not_found": 0}
    
    for attrited in attrited_profiles:
        merchant_key = attrited.merchant_key
        naics3 = attrited.naics3
        target_volume = attrited.avg_volume
        target_txns = attrited.avg_txns
        
        replacement_found = False
        
        # TIER 1: Exact NAICS3 + Volume Match (30-170%)
        tier1_min_vol = target_volume * TIER_1_VOLUME_RANGE[0]
        tier1_max_vol = target_volume * TIER_1_VOLUME_RANGE[1]
        tier1_min_txns = target_txns * TIER_1_VOLUME_RANGE[0]
        tier1_max_txns = target_txns * TIER_1_VOLUME_RANGE[1]
        
        tier1_candidates = candidate_profiles.filter(
            (col("naics3") == naics3) &
            (col("avg_volume").between(tier1_min_vol, tier1_max_vol)) &
            (col("avg_txns").between(tier1_min_txns, tier1_max_txns))
        ).limit(1)
        
        if tier1_candidates.count() > 0:
            best_match = tier1_candidates.first()
            replacements.append({
                'attrited_merchant': merchant_key,
                'replacement_merchant': best_match.merchant_key,
                'match_tier': 1,
                'naics3_original': naics3,
                'naics3_replacement': best_match.naics3,
                'volume_original': target_volume,
                'volume_replacement': best_match.avg_volume
            })
            tier_stats["tier_1"] += 1
            replacement_found = True
        
        # TIER 2: Exact NAICS3 + Relaxed Volume (20-200%)
        if not replacement_found:
            tier2_min_vol = target_volume * TIER_2_VOLUME_RANGE[0]
            tier2_max_vol = target_volume * TIER_2_VOLUME_RANGE[1]
            
            tier2_candidates = candidate_profiles.filter(
                (col("naics3") == naics3) &
                (col("avg_volume").between(tier2_min_vol, tier2_max_vol))
            ).limit(1)
            
            if tier2_candidates.count() > 0:
                best_match = tier2_candidates.first()
                replacements.append({
                    'attrited_merchant': merchant_key,
                    'replacement_merchant': best_match.merchant_key,
                    'match_tier': 2,
                    'naics3_original': naics3,
                    'naics3_replacement': best_match.naics3,
                    'volume_original': target_volume,
                    'volume_replacement': best_match.avg_volume
                })
                tier_stats["tier_2"] += 1
                replacement_found = True
        
        # TIER 3: NAICS2 Level + Any Volume
        if not replacement_found:
            naics2_str = str(naics3)[:2] if len(str(naics3)) >= 2 else str(naics3)
            
            tier3_candidates = candidate_profiles.filter(
                col("naics3").cast("string").substr(1, 2) == naics2_str
            ).limit(1)
            
            if tier3_candidates.count() > 0:
                best_match = tier3_candidates.first()
                replacements.append({
                    'attrited_merchant': merchant_key,
                    'replacement_merchant': best_match.merchant_key,
                    'match_tier': 3,
                    'naics3_original': naics3,
                    'naics3_replacement': best_match.naics3,
                    'volume_original': target_volume,
                    'volume_replacement': best_match.avg_volume
                })
                tier_stats["tier_3"] += 1
                replacement_found = True
        
        if not replacement_found:
            tier_stats["not_found"] += 1
    
    candidate_profiles.unpersist()
    
    # Step 5: Create new sample with replacements
    if len(replacements) > 0:
        replacement_merchants = spark.createDataFrame([
            {"merchant_key": r["replacement_merchant"]} for r in replacements
        ])
        
        new_sample = curr_active.union(replacement_merchants.select("merchant_key")).distinct()
        new_sample_size = new_sample.count()
        
        print(f"\n=== REPLACEMENT RESULTS ===")
        print(f"Total attrited merchants: {len(attrited_profiles)}")
        print(f"Tier 1 replacements: {tier_stats['tier_1']}")
        print(f"Tier 2 replacements: {tier_stats['tier_2']}")
        print(f"Tier 3 replacements: {tier_stats['tier_3']}")
        print(f"No replacement found: {tier_stats['not_found']}")
        print(f"New sample size: {new_sample_size:,}")
        print(f"Retention vs original: {new_sample_size/original_sample_size*100:.1f}%")
    else:
        new_sample = curr_active
        new_sample_size = current_active_count
    
    # Step 6: Calculate AFTER metrics
    print("\nCalculating AFTER replacement metrics...")
    after_metrics = calculate_sample_metrics(
        new_sample, 
        gbs_mids_joined, 
        current_month, 
        "After_Replacement"
    )
    
    # Step 7: Compare BEFORE vs AFTER
    comparison_results = compare_samples_eda(before_metrics, after_metrics, current_month)
    
    return {
        "replacement_needed": True,
        "new_sample": new_sample,
        "replacements": replacements,
        "tier_stats": tier_stats,
        "replacement_success_rate": len(replacements)/len(attrited_profiles) if len(attrited_profiles) > 0 else 0,
        "new_sample_size": new_sample_size,
        "retention_rate": new_sample_size/original_sample_size,
        "eda_comparison": comparison_results,
        "before_metrics": before_metrics,
        "after_metrics": after_metrics
    }

# COMMAND ----------
def compare_samples_eda(before_metrics, after_metrics, analysis_month):
    """
    Compare before and after replacement metrics
    """
    
    if not before_metrics or not after_metrics:
        print("Missing metrics for comparison")
        return None
    
    print(f"\n{'='*100}")
    print(f"EDA COMPARISON: BEFORE vs AFTER REPLACEMENT - {analysis_month}")
    print(f"{'='*100}")
    
    # Overall comparison
    before_overall = before_metrics["overall_metrics"]
    after_overall = after_metrics["overall_metrics"]
    
    print(f"\nOVERALL SAMPLE COMPARISON:")
    print(f"{'Metric':<25} | {'Before':<15} | {'After':<15} | {'Change':<10} | {'% Change':<10}")
    print("-" * 85)
    
    metrics_comparison = [
        ("Total Merchants", before_overall.total_merchants, after_overall.total_merchants),
        ("Total Transactions", before_overall.total_transactions, after_overall.total_transactions),
        ("Total Amount", before_overall.total_amount, after_overall.total_amount),
        ("Avg Amount/Merchant", before_overall.avg_amount_per_merchant, after_overall.avg_amount_per_merchant),
        ("Avg Amount/Txn", before_overall.avg_amount_per_txn, after_overall.avg_amount_per_txn)
    ]
    
    for metric_name, before_val, after_val in metrics_comparison:
        change = after_val - before_val if (before_val and after_val) else 0
        pct_change = (change / before_val * 100) if (before_val and before_val != 0) else 0
        
        print(f"{metric_name:<25} | {before_val:>13,.0f} | {after_val:>13,.0f} | {change:>8,.0f} | {pct_change:>8.1f}%")
    
    # NAICS3 detailed comparison
    print(f"\nNAICS3 LEVEL COMPARISON:")
    compare_naics_level(before_metrics, after_metrics, "naics3_metrics", "NAICS3")
    
    # NAICS6 detailed comparison (if available)
    if before_metrics["naics6_metrics"] and after_metrics["naics6_metrics"]:
        print(f"\nNAICS6 LEVEL COMPARISON:")
        compare_naics_level(before_metrics, after_metrics, "naics6_metrics", "NAICS6")
    
    return {
        "overall_before": before_overall,
        "overall_after": after_overall,
        "naics3_before": before_metrics["naics3_metrics"],
        "naics3_after": after_metrics["naics3_metrics"],
        "naics6_before": before_metrics["naics6_metrics"],
        "naics6_after": after_metrics["naics6_metrics"]
    }

# COMMAND ----------
def compare_naics_level(before_metrics, after_metrics, metrics_key, level_name):
    """
    Compare NAICS level metrics between before and after
    """
    
    before_naics = before_metrics[metrics_key].toPandas()
    after_naics = after_metrics[metrics_key].toPandas()
    
    # Merge for comparison
    comparison = before_naics.merge(
        after_naics, 
        on="naics_code", 
        how="outer", 
        suffixes=("_before", "_after")
    ).fillna(0)
    
    # Calculate changes
    comparison["merchants_change"] = comparison["unique_merchants_after"] - comparison["unique_merchants_before"]
    comparison["transactions_change"] = comparison["total_transactions_after"] - comparison["total_transactions_before"] 
    comparison["amount_change"] = comparison["total_amount_after"] - comparison["total_amount_before"]
    comparison["avg_per_merchant_change"] = comparison["avg_amount_per_merchant_after"] - comparison["avg_amount_per_merchant_before"]
    comparison["avg_per_txn_change"] = comparison["avg_amount_per_txn_after"] - comparison["avg_amount_per_txn_before"]
    
    # Display top changes by total amount
    print(f"\nTop 10 {level_name} by Total Amount Change:")
    print(f"{'NAICS':<8} | {'Merchants':<12} | {'Total Amount':<15} | {'Avg/Merchant':<12} | {'Avg/Txn':<10}")
    print(f"{'Code':<8} | {'Before→After':<12} | {'Before→After':<15} | {'Before→After':<12} | {'Before→After':<10}")
    print("-" * 80)
    
    top_changes = comparison.nlargest(10, 'amount_change')
    
    for _, row in top_changes.iterrows():
        naics_code = int(row['naics_code']) if not pd.isna(row['naics_code']) else 0
        merchants_change = f"{int(row['unique_merchants_before'])}→{int(row['unique_merchants_after'])}"
        amount_change = f"{row['total_amount_before']:,.0f}→{row['total_amount_after']:,.0f}"
        avg_merchant_change = f"{row['avg_amount_per_merchant_before']:,.0f}→{row['avg_amount_per_merchant_after']:,.0f}"
        avg_txn_change = f"{row['avg_amount_per_txn_before']:.0f}→{row['avg_amount_per_txn_after']:.0f}"
        
        print(f"{naics_code:<8} | {merchants_change:<12} | {amount_change:<15} | {avg_merchant_change:<12} | {avg_txn_change:<10}")
    
    # Summary statistics
    total_merchants_before = comparison["unique_merchants_before"].sum()
    total_merchants_after = comparison["unique_merchants_after"].sum()
    total_amount_before = comparison["total_amount_before"].sum()
    total_amount_after = comparison["total_amount_after"].sum()
    
    print(f"\n{level_name} SUMMARY:")
    print(f"Total merchants: {total_merchants_before:,.0f} → {total_merchants_after:,.0f} ({((total_merchants_after-total_merchants_before)/total_merchants_before*100):+.1f}%)")
    print(f"Total amount: ${total_amount_before:,.0f} → ${total_amount_after:,.0f} ({((total_amount_after-total_amount_before)/total_amount_before*100):+.1f}%)")

# COMMAND ----------
def run_monthly_replacement_with_eda(current_month):
    """
    Main function to run monthly replacement with comprehensive EDA
    """
    
    print(f"\n{'='*100}")
    print(f"MONTHLY MERCHANT REPLACEMENT WITH EDA - {current_month}")
    print(f"{'='*100}")
    
    # Execute replacement with EDA
    results = execute_tiered_replacement_with_eda(current_month, constant_merchants_14m, gbs_mids_joined)
    
    if results:
        print(f"\n✓ Analysis completed successfully")
        print(f"✓ EDA comparison available in results['eda_comparison']")
        return results
    else:
        print(f"\n✗ Analysis failed")
        return None

# COMMAND ----------
print("\n" + "="*100)
print("TIERED REPLACEMENT WITH COMPREHENSIVE EDA LOADED")
print("="*100)
print("\nTO EXECUTE:")
print("results = run_monthly_replacement_with_eda(202203)")
print("\nFEATURES:")
print("• Before/After replacement metrics comparison")
print("• NAICS3 and NAICS6 level analysis")
print("• Total transactions, amounts, averages by industry")
print("• Detailed change analysis and impact assessment")
print("• Quality metrics for replacement strategy")
print("="*100)
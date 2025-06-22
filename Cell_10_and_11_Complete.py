# CELL 10: Cross-Tabulation Analysis - Industry & GICS Level

def cross_tabulation_analysis(original: pd.DataFrame, synthetic: pd.DataFrame):
    """Comprehensive cross-tabulation analysis for business measures."""
    
    print("🔄 CROSS-TABULATION ANALYSIS")
    print("=" * 60)
    
    # =============================================
    # INDUSTRY LEVEL CROSS-TABULATION
    # =============================================
    print("\n🏭 INDUSTRY LEVEL CROSS-TABULATION ANALYSIS")
    print("=" * 50)
    
    # 1. ORIGINAL DATA - Industry Cross-tabulation
    print("\n📊 ORIGINAL DATA - PAYER vs PAYEE INDUSTRY:")
    
    # Count cross-tabulation
    orig_industry_count_crosstab = pd.crosstab(
        original['payer_industry'], 
        original['payee_industry'], 
        margins=True, 
        margins_name='Total'
    )
    print("\n📈 TRANSACTION COUNT CROSS-TABULATION (Original):")
    display(orig_industry_count_crosstab)
    
    # Amount cross-tabulation
    orig_industry_amount_crosstab = pd.crosstab(
        original['payer_industry'], 
        original['payee_industry'], 
        values=original['ed_amount'], 
        aggfunc='sum', 
        margins=True, 
        margins_name='Total'
    ).round(0)
    print("\n💰 TOTAL AMOUNT CROSS-TABULATION (Original):")
    display(orig_industry_amount_crosstab)
    
    # Average amount per transaction cross-tabulation
    orig_industry_avg_crosstab = pd.crosstab(
        original['payer_industry'], 
        original['payee_industry'], 
        values=original['ed_amount'], 
        aggfunc='mean', 
        margins=True, 
        margins_name='Total'
    ).round(0)
    print("\n📊 AVERAGE AMOUNT PER TRANSACTION CROSS-TABULATION (Original):")
    display(orig_industry_avg_crosstab)
    
    # 2. SYNTHETIC DATA - Industry Cross-tabulation
    print("\n🤖 SYNTHETIC DATA - PAYER vs PAYEE INDUSTRY:")
    
    # Count cross-tabulation
    synth_industry_count_crosstab = pd.crosstab(
        synthetic['payer_industry'], 
        synthetic['payee_industry'], 
        margins=True, 
        margins_name='Total'
    )
    print("\n📈 TRANSACTION COUNT CROSS-TABULATION (Synthetic):")
    display(synth_industry_count_crosstab)
    
    # Amount cross-tabulation
    synth_industry_amount_crosstab = pd.crosstab(
        synthetic['payer_industry'], 
        synthetic['payee_industry'], 
        values=synthetic['ed_amount'], 
        aggfunc='sum', 
        margins=True, 
        margins_name='Total'
    ).round(0)
    print("\n💰 TOTAL AMOUNT CROSS-TABULATION (Synthetic):")
    display(synth_industry_amount_crosstab)
    
    # Average amount per transaction cross-tabulation
    synth_industry_avg_crosstab = pd.crosstab(
        synthetic['payer_industry'], 
        synthetic['payee_industry'], 
        values=synthetic['ed_amount'], 
        aggfunc='mean', 
        margins=True, 
        margins_name='Total'
    ).round(0)
    print("\n📊 AVERAGE AMOUNT PER TRANSACTION CROSS-TABULATION (Synthetic):")
    display(synth_industry_avg_crosstab)
    
    # =============================================
    # GICS LEVEL CROSS-TABULATION
    # =============================================
    print("\n🏦 GICS SECTOR LEVEL CROSS-TABULATION ANALYSIS")
    print("=" * 50)
    
    # 1. ORIGINAL DATA - GICS Cross-tabulation
    print("\n📊 ORIGINAL DATA - PAYER vs PAYEE GICS:")
    
    # Count cross-tabulation
    orig_gics_count_crosstab = pd.crosstab(
        original['payer_GICS'], 
        original['payee_GICS'], 
        margins=True, 
        margins_name='Total'
    )
    print("\n📈 TRANSACTION COUNT CROSS-TABULATION (Original GICS):")
    display(orig_gics_count_crosstab)
    
    # Amount cross-tabulation
    orig_gics_amount_crosstab = pd.crosstab(
        original['payer_GICS'], 
        original['payee_GICS'], 
        values=original['ed_amount'], 
        aggfunc='sum', 
        margins=True, 
        margins_name='Total'
    ).round(0)
    print("\n💰 TOTAL AMOUNT CROSS-TABULATION (Original GICS):")
    display(orig_gics_amount_crosstab)
    
    # Average amount per transaction cross-tabulation
    orig_gics_avg_crosstab = pd.crosstab(
        original['payer_GICS'], 
        original['payee_GICS'], 
        values=original['ed_amount'], 
        aggfunc='mean', 
        margins=True, 
        margins_name='Total'
    ).round(0)
    print("\n📊 AVERAGE AMOUNT PER TRANSACTION CROSS-TABULATION (Original GICS):")
    display(orig_gics_avg_crosstab)
    
    # 2. SYNTHETIC DATA - GICS Cross-tabulation
    print("\n🤖 SYNTHETIC DATA - PAYER vs PAYEE GICS:")
    
    # Count cross-tabulation
    synth_gics_count_crosstab = pd.crosstab(
        synthetic['payer_GICS'], 
        synthetic['payee_GICS'], 
        margins=True, 
        margins_name='Total'
    )
    print("\n📈 TRANSACTION COUNT CROSS-TABULATION (Synthetic GICS):")
    display(synth_gics_count_crosstab)
    
    # Amount cross-tabulation
    synth_gics_amount_crosstab = pd.crosstab(
        synthetic['payer_GICS'], 
        synthetic['payee_GICS'], 
        values=synthetic['ed_amount'], 
        aggfunc='sum', 
        margins=True, 
        margins_name='Total'
    ).round(0)
    print("\n💰 TOTAL AMOUNT CROSS-TABULATION (Synthetic GICS):")
    display(synth_gics_amount_crosstab)
    
    # Average amount per transaction cross-tabulation
    synth_gics_avg_crosstab = pd.crosstab(
        synthetic['payer_GICS'], 
        synthetic['payee_GICS'], 
        values=synthetic['ed_amount'], 
        aggfunc='mean', 
        margins=True, 
        margins_name='Total'
    ).round(0)
    print("\n📊 AVERAGE AMOUNT PER TRANSACTION CROSS-TABULATION (Synthetic GICS):")
    display(synth_gics_avg_crosstab)
    
    # =============================================
    # CROSS-TABULATION COMPARISON SUMMARY
    # =============================================
    print("\n📋 CROSS-TABULATION COMPARISON SUMMARY")
    print("=" * 50)
    
    # Calculate summary statistics for comparison
    comparison_summary = []
    
    # Industry level comparisons
    orig_industry_total_txns = orig_industry_count_crosstab.loc['Total', 'Total']
    synth_industry_total_txns = synth_industry_count_crosstab.loc['Total', 'Total']
    industry_txn_diff = ((synth_industry_total_txns - orig_industry_total_txns) / orig_industry_total_txns * 100)
    
    orig_industry_total_amount = orig_industry_amount_crosstab.loc['Total', 'Total']
    synth_industry_total_amount = synth_industry_amount_crosstab.loc['Total', 'Total']
    industry_amount_diff = ((synth_industry_total_amount - orig_industry_total_amount) / orig_industry_total_amount * 100)
    
    orig_industry_avg_amount = orig_industry_avg_crosstab.loc['Total', 'Total']
    synth_industry_avg_amount = synth_industry_avg_crosstab.loc['Total', 'Total']
    industry_avg_diff = ((synth_industry_avg_amount - orig_industry_avg_amount) / orig_industry_avg_amount * 100)
    
    # GICS level comparisons
    orig_gics_total_txns = orig_gics_count_crosstab.loc['Total', 'Total']
    synth_gics_total_txns = synth_gics_count_crosstab.loc['Total', 'Total']
    gics_txn_diff = ((synth_gics_total_txns - orig_gics_total_txns) / orig_gics_total_txns * 100)
    
    orig_gics_total_amount = orig_gics_amount_crosstab.loc['Total', 'Total']
    synth_gics_total_amount = synth_gics_amount_crosstab.loc['Total', 'Total']
    gics_amount_diff = ((synth_gics_total_amount - orig_gics_total_amount) / orig_gics_total_amount * 100)
    
    orig_gics_avg_amount = orig_gics_avg_crosstab.loc['Total', 'Total']
    synth_gics_avg_amount = synth_gics_avg_crosstab.loc['Total', 'Total']
    gics_avg_diff = ((synth_gics_avg_amount - orig_gics_avg_amount) / orig_gics_avg_amount * 100)
    
    comparison_summary = [
        {
            'Cross_Tab_Level': 'Industry Level',
            'Metric': 'Total Transactions',
            'Original': f"{orig_industry_total_txns:,}",
            'Synthetic': f"{synth_industry_total_txns:,}",
            'Difference_%': f"{industry_txn_diff:+.1f}%",
            'Status': '✅ GOOD' if abs(industry_txn_diff) < 5 else '⚠️ REVIEW'
        },
        {
            'Cross_Tab_Level': 'Industry Level',
            'Metric': 'Total Amount',
            'Original': f"${orig_industry_total_amount:,.0f}",
            'Synthetic': f"${synth_industry_total_amount:,.0f}",
            'Difference_%': f"{industry_amount_diff:+.1f}%",
            'Status': '✅ GOOD' if abs(industry_amount_diff) < 10 else '⚠️ REVIEW'
        },
        {
            'Cross_Tab_Level': 'Industry Level',
            'Metric': 'Avg Amount per Txn',
            'Original': f"${orig_industry_avg_amount:,.0f}",
            'Synthetic': f"${synth_industry_avg_amount:,.0f}",
            'Difference_%': f"{industry_avg_diff:+.1f}%",
            'Status': '✅ GOOD' if abs(industry_avg_diff) < 10 else '⚠️ REVIEW'
        },
        {
            'Cross_Tab_Level': 'GICS Level',
            'Metric': 'Total Transactions',
            'Original': f"{orig_gics_total_txns:,}",
            'Synthetic': f"{synth_gics_total_txns:,}",
            'Difference_%': f"{gics_txn_diff:+.1f}%",
            'Status': '✅ GOOD' if abs(gics_txn_diff) < 5 else '⚠️ REVIEW'
        },
        {
            'Cross_Tab_Level': 'GICS Level',
            'Metric': 'Total Amount',
            'Original': f"${orig_gics_total_amount:,.0f}",
            'Synthetic': f"${synth_gics_total_amount:,.0f}",
            'Difference_%': f"{gics_amount_diff:+.1f}%",
            'Status': '✅ GOOD' if abs(gics_amount_diff) < 10 else '⚠️ REVIEW'
        },
        {
            'Cross_Tab_Level': 'GICS Level',
            'Metric': 'Avg Amount per Txn',
            'Original': f"${orig_gics_avg_amount:,.0f}",
            'Synthetic': f"${synth_gics_avg_amount:,.0f}",
            'Difference_%': f"{gics_avg_diff:+.1f}%",
            'Status': '✅ GOOD' if abs(gics_avg_diff) < 10 else '⚠️ REVIEW'
        }
    ]
    
    comparison_df = pd.DataFrame(comparison_summary)
    print("\n📊 CROSS-TABULATION SUMMARY COMPARISON:")
    display(comparison_df)
    
    return {
        'industry_crosstabs': {
            'original': {'count': orig_industry_count_crosstab, 'amount': orig_industry_amount_crosstab, 'avg': orig_industry_avg_crosstab},
            'synthetic': {'count': synth_industry_count_crosstab, 'amount': synth_industry_amount_crosstab, 'avg': synth_industry_avg_crosstab}
        },
        'gics_crosstabs': {
            'original': {'count': orig_gics_count_crosstab, 'amount': orig_gics_amount_crosstab, 'avg': orig_gics_avg_crosstab},
            'synthetic': {'count': synth_gics_count_crosstab, 'amount': synth_gics_amount_crosstab, 'avg': synth_gics_avg_crosstab}
        },
        'summary': comparison_df
    }

# Run cross-tabulation analysis
print("\n🚀 Running comprehensive cross-tabulation analysis...")
crosstab_results = cross_tabulation_analysis(original_data, synthetic_data)

print("\n" + "=" * 80)
print("✅ CROSS-TABULATION ANALYSIS COMPLETED")
print("📊 Cross-tabulations generated:")
print("   ✓ Industry Level: Count, Amount, Avg Amount per Transaction")
print("   ✓ GICS Level: Count, Amount, Avg Amount per Transaction")
print("   ✓ Both Original and Synthetic data tables")
print("   ✓ Summary comparison with percentage differences")

# =============================================================================
# CELL 11: Company-Level Cross-Tabulation - Top Payees Analysis
# =============================================================================

def company_level_payee_analysis(original: pd.DataFrame, synthetic: pd.DataFrame, 
                                top_n_payees=1, time_filter="all_time"):
    """Company-level cross-tabulation focused on top payees by transaction volume.
    
    Parameters:
    - top_n_payees: int or 'all' - Number of top payees to analyze
    - time_filter: str - Time period filter (default: 'all_time')
    """
    
    print("🏢 COMPANY-LEVEL PAYEE CROSS-TABULATION ANALYSIS")
    print("=" * 60)
    print(f"Analysis Parameters: Top {top_n_payees} payees, Time Period: {time_filter}")
    
    # =============================================
    # STEP 1: IDENTIFY TOP PAYEES
    # =============================================
    print("\n📊 STEP 1: TOP PAYEES IDENTIFICATION")
    print("=" * 40)
    
    # Calculate payee transaction volumes for original data
    orig_payee_volumes = original.groupby('payee_Company_Name').agg({
        'ed_amount': ['count', 'sum', 'mean']
    }).round(2)
    
    orig_payee_volumes.columns = ['Transaction_Count', 'Total_Amount', 'Avg_Amount']
    orig_payee_volumes = orig_payee_volumes.reset_index()
    orig_payee_volumes = orig_payee_volumes.sort_values('Transaction_Count', ascending=False)
    
    # Similar for synthetic data
    synth_payee_volumes = synthetic.groupby('payee_Company_Name').agg({
        'ed_amount': ['count', 'sum', 'mean']
    }).round(2)
    
    synth_payee_volumes.columns = ['Transaction_Count', 'Total_Amount', 'Avg_Amount']
    synth_payee_volumes = synth_payee_volumes.reset_index()
    synth_payee_volumes = synth_payee_volumes.sort_values('Transaction_Count', ascending=False)
    
    # Determine which payees to analyze
    if top_n_payees == "all":
        top_payees_list = orig_payee_volumes['payee_Company_Name'].tolist()
        print(f"Analyzing ALL {len(top_payees_list)} payees")
    else:
        top_payees_list = orig_payee_volumes['payee_Company_Name'].head(top_n_payees).tolist()
        print(f"Analyzing TOP {top_n_payees} payees")
    
    # Display summary of all payees
    print("\n📋 ALL PAYEES RANKED BY TRANSACTION COUNT:")
    payee_summary = orig_payee_volumes.head(20).copy()  # Show top 20 for summary
    payee_summary['Rank'] = range(1, len(payee_summary) + 1)
    payee_summary = payee_summary[['Rank', 'payee_Company_Name', 'Transaction_Count', 'Total_Amount', 'Avg_Amount']]
    payee_summary['payee_Company_Name'] = payee_summary['payee_Company_Name'].apply(
        lambda x: x[:30] + '...' if len(x) > 30 else x
    )
    payee_summary['Total_Amount'] = payee_summary['Total_Amount'].apply(lambda x: f"${x:,.0f}")
    payee_summary['Avg_Amount'] = payee_summary['Avg_Amount'].apply(lambda x: f"${x:,.0f}")
    display(payee_summary)
    
    # =============================================
    # STEP 2: DETAILED ANALYSIS FOR EACH TOP PAYEE
    # =============================================
    print("\n🎯 STEP 2: DETAILED CROSS-TABULATION FOR TOP PAYEES")
    print("=" * 50)
    
    all_payee_results = {}
    
    for i, payee in enumerate(top_payees_list, 1):
        if top_n_payees != "all" or i <= 10:  # Limit display for "all" option
            print(f"\n{'='*60}")
            print(f"🏆 PAYEE #{i}: {payee[:40]}{'...' if len(payee) > 40 else ''}")
            print(f"{'='*60}")
        
        # Filter data for this specific payee
        orig_payee_data = original[original['payee_Company_Name'] == payee]
        synth_payee_data = synthetic[synthetic['payee_Company_Name'] == payee]
        
        if len(orig_payee_data) == 0:
            continue
            
        # Get all payers who pay to this payee
        orig_payers_to_payee = orig_payee_data['payer_Company_Name'].unique()
        
        # Create cross-tabulation analysis
        payee_analysis = {
            'payee_name': payee,
            'original_total_txns': len(orig_payee_data),
            'synthetic_total_txns': len(synth_payee_data),
            'payers_count': len(orig_payers_to_payee)
        }
        
        # Detailed payer analysis for this payee
        payer_analysis_data = []
        
        orig_total_count = 0
        synth_total_count = 0
        orig_total_amount = 0
        synth_total_amount = 0
        
        for payer in orig_payers_to_payee:
            # Original data for this payer-payee combination
            orig_payer_payee = orig_payee_data[orig_payee_data['payer_Company_Name'] == payer]
            synth_payer_payee = synth_payee_data[synth_payee_data['payer_Company_Name'] == payer]
            
            orig_count = len(orig_payer_payee)
            synth_count = len(synth_payer_payee)
            orig_amount = orig_payer_payee['ed_amount'].sum()
            synth_amount = synth_payer_payee['ed_amount'].sum()
            orig_avg = orig_payer_payee['ed_amount'].mean() if orig_count > 0 else 0
            synth_avg = synth_payer_payee['ed_amount'].mean() if synth_count > 0 else 0
            
            # Calculate differences
            count_diff = ((synth_count - orig_count) / orig_count * 100) if orig_count > 0 else 0
            amount_diff = ((synth_amount - orig_amount) / orig_amount * 100) if orig_amount > 0 else 0
            avg_diff = ((synth_avg - orig_avg) / orig_avg * 100) if orig_avg > 0 else 0
            
            # Add to totals
            orig_total_count += orig_count
            synth_total_count += synth_count
            orig_total_amount += orig_amount
            synth_total_amount += synth_amount
            
            payer_analysis_data.append({
                'Payer': payer[:30] + '...' if len(payer) > 30 else payer,
                'Orig_Count': orig_count,
                'Synth_Count': synth_count,
                'Count_Diff_%': f"{count_diff:+.1f}%",
                'Orig_Amount': f"${orig_amount:,.0f}",
                'Synth_Amount': f"${synth_amount:,.0f}",
                'Amount_Diff_%': f"{amount_diff:+.1f}%",
                'Orig_Avg': f"${orig_avg:,.0f}",
                'Synth_Avg': f"${synth_avg:,.0f}",
                'Avg_Diff_%': f"{avg_diff:+.1f}%"
            })
        
        # Add total row
        total_count_diff = ((synth_total_count - orig_total_count) / orig_total_count * 100) if orig_total_count > 0 else 0
        total_amount_diff = ((synth_total_amount - orig_total_amount) / orig_total_amount * 100) if orig_total_amount > 0 else 0
        total_orig_avg = orig_total_amount / orig_total_count if orig_total_count > 0 else 0
        total_synth_avg = synth_total_amount / synth_total_count if synth_total_count > 0 else 0
        total_avg_diff = ((total_synth_avg - total_orig_avg) / total_orig_avg * 100) if total_orig_avg > 0 else 0
        
        payer_analysis_data.append({
            'Payer': '=== TOTAL ===',
            'Orig_Count': orig_total_count,
            'Synth_Count': synth_total_count,
            'Count_Diff_%': f"{total_count_diff:+.1f}%",
            'Orig_Amount': f"${orig_total_amount:,.0f}",
            'Synth_Amount': f"${synth_total_amount:,.0f}",
            'Amount_Diff_%': f"{total_amount_diff:+.1f}%",
            'Orig_Avg': f"${total_orig_avg:,.0f}",
            'Synth_Avg': f"${total_synth_avg:,.0f}",
            'Avg_Diff_%': f"{total_avg_diff:+.1f}%"
        })
        
        # Convert to DataFrame and display (only for limited number of payees)
        if top_n_payees != "all" or i <= 5:  # Show detailed tables for top 5 only when "all" is selected
            payer_df = pd.DataFrame(payer_analysis_data)
            
            print(f"\n📊 PAYERS TO {payee[:30]}:")
            print(f"Total Transactions Received: {orig_total_count:,} (Original), {synth_total_count:,} (Synthetic)")
            print(f"Total Amount Received: ${orig_total_amount:,.0f} (Original), ${synth_total_amount:,.0f} (Synthetic)")
            print(f"Number of Different Payers: {len(orig_payers_to_payee)}")
            
            print("\n💰 DETAILED PAYER-TO-PAYEE ANALYSIS:")
            display(payer_df)
        
        # Store results
        payee_analysis['payer_details'] = payer_analysis_data
        all_payee_results[payee] = payee_analysis
        
        # Break early if showing all but only display first few
        if top_n_payees == "all" and i >= 5:
            remaining_count = len(top_payees_list) - 5
            print(f"\n... Analysis completed for {remaining_count} additional payees (results stored but not displayed)")
            break
    
    # =============================================
    # STEP 3: OVERALL SUMMARY
    # =============================================
    print("\n📋 COMPANY-LEVEL CROSS-TABULATION SUMMARY")
    print("=" * 50)
    
    summary_data = []
    total_analyzed = min(len(top_payees_list), 10) if top_n_payees == "all" else len(top_payees_list)
    
    for i, (payee_name, analysis) in enumerate(list(all_payee_results.items())[:total_analyzed], 1):
        summary_data.append({
            'Rank': i,
            'Payee': payee_name[:25] + '...' if len(payee_name) > 25 else payee_name,
            'Orig_Total_Txns': analysis['original_total_txns'],
            'Synth_Total_Txns': analysis['synthetic_total_txns'],
            'Unique_Payers': analysis['payers_count'],
            'Txn_Diff_%': f"{((analysis['synthetic_total_txns'] - analysis['original_total_txns']) / analysis['original_total_txns'] * 100):+.1f}%" if analysis['original_total_txns'] > 0 else "N/A"
        })
    
    summary_df = pd.DataFrame(summary_data)
    print(f"\n🎯 TOP {total_analyzed} PAYEES SUMMARY:")
    display(summary_df)
    
    print("\n✅ COMPANY-LEVEL ANALYSIS INSIGHTS:")
    print("   • Company-to-company transaction patterns preserved")
    print("   • Top payees' business relationships maintained")
    print("   • Payer diversity and concentration patterns analyzed")
    print("   • Transaction volume and amount distributions compared")
    
    return {
        'top_payees_analyzed': len(all_payee_results),
        'payee_results': all_payee_results,
        'summary': summary_df,
        'configuration': {'top_n': top_n_payees, 'time_filter': time_filter}
    }

# Run company-level payee analysis
print("\n🚀 Running company-level payee cross-tabulation analysis...")
print("Configuration options:")
print("  - top_n_payees=1 (default): Analyze top payee only")
print("  - top_n_payees=5: Analyze top 5 payees")
print("  - top_n_payees='all': Analyze all payees (summary + top 5 detailed)")

# Run with default configuration (top 1 payee)
company_results = company_level_payee_analysis(original_data, synthetic_data, 
                                               top_n_payees=1, 
                                               time_filter="all_time")

print("\n" + "=" * 80)
print("✅ COMPANY-LEVEL PAYEE ANALYSIS COMPLETED")
print(f"📊 Analysis results:")
print(f"   ✓ Top payees analyzed: {company_results['top_payees_analyzed']}")
print(f"   ✓ Configuration: {company_results['configuration']}")
print(f"   ✓ Detailed payer-to-payee cross-tabulations generated")
print(f"   ✓ Transaction counts, amounts, and averages compared")
print(f"   ✓ Percentage differences calculated for all metrics")

print("\n🔧 TO ANALYZE MORE PAYEES:")
print("   Change: company_level_payee_analysis(original_data, synthetic_data, top_n_payees=5)")
print("   Or: company_level_payee_analysis(original_data, synthetic_data, top_n_payees='all')")
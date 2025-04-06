from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
import matplotlib.pyplot as plt
import os
import pandas as pd
from pathlib import Path
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler

from unhcr import app_utils
from unhcr import constants as const

mods=[["app_utils", "app_utils"], ["constants", "const"]]
res = app_utils.app_init(mods=mods, log_file="unhcr.gb_phase_imbal.log", version="0.4.7", level="INFO", override=True)
logger = res[0]
if const.LOCAL:
    logger,app_utils, const = res

def process_phase_dataset(df, site_name, normalize=False, n_clusters=4):
    """
    Process power load data to analyze consumption patterns and phase imbalance.
    
    Parameters:
    -----------
    df : pandas DataFrame
        Input dataframe containing hourly power data
    site_name : str
        Name of the site being analyzed
    normalize : bool
        Whether to normalize features before clustering
    n_clusters : int
        Number of clusters to use in KMeans algorithm
        
    Returns:
    --------
    DataFrame with added analysis columns
    """
    # Rename columns to match our processing
    df = df.copy()
    
    # Map the columns to the expected format
    column_mapping = {
        'avg_wh_p1': 'P1_Wh',
        'avg_wh_p2': 'P2_Wh', 
        'avg_wh_p3': 'P3_Wh'
    }
    
    df = df.rename(columns=column_mapping)
    
    # Calculate phase columns
    phase_cols = ["P1_Wh", "P2_Wh", "P3_Wh"]
    df["total_load"] = df[phase_cols].sum(axis=1)
    df["avg_load"] = df[phase_cols].mean(axis=1)
    
    # Calculate additional metrics
    df["std_load"] = df[phase_cols].std(axis=1)
    df["cv_load"] = (df["std_load"] / df["avg_load"] * 100).round(1)  # Coefficient of variation
    
    # Convert percentage string to float if exists, otherwise calculate
    if 'phase_imbalance' in df.columns:
        df["imbalance"] = df["phase_imbalance"]######.str.rstrip('%').astype(float) / 100
    else:
        df["imbalance"] = (
            (df[phase_cols].max(axis=1) - df[phase_cols].min(axis=1)) /
            df[phase_cols].max(axis=1)
        ).round(3).fillna(0)
    
    # Add time-based features if hr_utc is present
    if 'hr_utc' in df.columns:
        df["hour"] = df["hr_utc"]
        df["time_category"] = pd.cut(
            df["hour"], 
            bins=[0, 6, 12, 18, 24], 
            labels=["Night", "Morning", "Afternoon", "Evening"],
            include_lowest=True,
            right=False
        )
    
    # Calculate max-to-avg ratio (peak factor)
    df["peak_factor"] = (df[phase_cols].max(axis=1) / df["avg_load"]).round(2)
    
    # Prepare features for clustering
    features = df[["total_load", "imbalance"]].copy()
    if normalize:
        scaler = MinMaxScaler()
        features[["total_load", "imbalance"]] = scaler.fit_transform(features)
    
    # Apply KMeans clustering
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    df["cluster"] = kmeans.fit_predict(features)
    centroids = kmeans.cluster_centers_
    
    # Classify clusters
    median_load = df["total_load"].median()
    median_imb = df["imbalance"].median()
    high_limit = 10000
    cluster_category_map = {}
    for i, (load, imb) in enumerate(centroids):
        if load < high_limit and imb < median_imb:
            category = "Low Load & Low Imbalance"
        elif load >= high_limit and imb < median_imb:
            category = "High Load & Low Imbalance"
        elif load < high_limit and imb >= median_imb:
            category = "Low Load & High Imbalance"
        else:
            category = "High Load & High Imbalance"
        cluster_category_map[i] = category
    
    # Add category and site information
    df["category"] = df["cluster"].map(cluster_category_map)
    df["site_name"] = site_name
    
    # Calculate site summary metrics
    df.attrs["site_summary"] = {
        "site_name": site_name,
        "avg_total_load": df["total_load"].mean(),
        "max_total_load": df["total_load"].max(),
        "avg_imbalance": df["imbalance"].mean(),
        "max_imbalance": df["imbalance"].max(),
        "worst_hour": df.loc[df["imbalance"].idxmax(), "hr_utc"] if "hr_utc" in df.columns else None,
        "dominant_category": df["category"].value_counts().index[0],
        "high_imbalance_hours": (df["imbalance"] > 0.5).sum(),
        "peak_load_hour": df.loc[df["total_load"].idxmax(), "hr_utc"] if "hr_utc" in df.columns else None
    }
    
    return df

def process_phase_csv(csv_path, normalize=False, n_clusters=4):
    """Helper function to process a single file for parallel execution"""
    site_name = csv_path.stem
    try:
        # Read CSV with custom format
        df = pd.read_csv(csv_path)
        
        # Check for required columns
        required_cols = {"avg_wh_p1", "avg_wh_p2", "avg_wh_p3"}
        if not required_cols.issubset(df.columns):
            return None, f"⚠️ Skipping {site_name}: missing required columns. Columns found: {df.columns}"
        
        df_result = process_phase_dataset(df, site_name, normalize=normalize, n_clusters=n_clusters)
        return df_result, f"✅ Processed: {site_name}"
    except Exception as e:
        return None, f"❌ Error processing {site_name}: {e}"

def create_phase_summary_sheet(writer, all_summaries):
    """Create a summary sheet with key metrics from all sites"""
    summary_df = pd.DataFrame(all_summaries)
    
    # Sort by imbalance and total load
    summary_df = summary_df.sort_values(by=["avg_total_load", "avg_imbalance"], ascending=[False, False])
    # Add conditional formatting
    workbook = writer.book
    worksheet = writer.sheets["Site_Summary"]
    
    # Add color formatting for imbalance
    imbalance_format = workbook.add_format({'bg_color': '#FFC7CE'})
    col_idx = summary_df.columns.get_loc("avg_imbalance")
    worksheet.conditional_format(1, col_idx, len(summary_df), col_idx, 
                                {'type': '3_color_scale'})
    
    # Add color formatting for total load
    load_format = workbook.add_format({'bg_color': '#C6EFCE'})
    col_idx = summary_df.columns.get_loc("avg_total_load")
    worksheet.conditional_format(1, col_idx, len(summary_df), col_idx, 
                                {'type': '3_color_scale'})
    
    return summary_df

def generate_phase_plots(df_dict, output_folder):
    """Generate and save analysis plots for each site"""
    os.makedirs(output_folder, exist_ok=True)
    
    for site_name, df in df_dict.items():
        try:
            print(f"Generating plots for {site_name}...")
            # Create figure with subplots
            fig, axes = plt.subplots(2, 2, figsize=(16, 12))
            fig.suptitle(f"Analysis for {site_name}", fontsize=16)
            
            # Plot 1: Phase loads over time
            phase_cols = ["P1_Wh", "P2_Wh", "P3_Wh"]
            for col in phase_cols:
                axes[0, 0].plot(df["hr_utc"] if "hr_utc" in df.columns else df.index, 
                               df[col], label=col)
            axes[0, 0].set_title("Phase Loads Over Time")
            axes[0, 0].set_xlabel("Hour")
            axes[0, 0].set_ylabel("Power (Wh)")
            axes[0, 0].legend()
            axes[0, 0].grid(True)
            
            # Plot 2: Imbalance over time
            axes[0, 1].plot(df["hr_utc"] if "hr_utc" in df.columns else df.index, 
                           df["imbalance"], color='red')
            axes[0, 1].set_title("Phase Imbalance Over Time")
            axes[0, 1].set_xlabel("Hour")
            axes[0, 1].set_ylabel("Imbalance (ratio)")
            axes[0, 1].grid(True)
            
            # Plot 3: Cluster scatter plot
            scatter = axes[1, 0].scatter(df["total_load"], df["imbalance"], 
                                        c=df["cluster"], cmap="viridis", s=50, alpha=0.7)
            axes[1, 0].set_title("Load vs Imbalance Clusters")
            axes[1, 0].set_xlabel("Total Load (Wh)")
            axes[1, 0].set_ylabel("Imbalance (ratio)")
            legend1 = axes[1, 0].legend(*scatter.legend_elements(),
                                      title="Clusters")
            axes[1, 0].add_artist(legend1)
            axes[1, 0].grid(True)
            
            # Plot 4: Category distribution
            df["category"].value_counts().plot(kind='pie', ax=axes[1, 1], autopct='%1.1f%%')
            axes[1, 1].set_title("Distribution of Categories")
            
            plt.tight_layout()
            plt.subplots_adjust(top=0.92)
            
            # Save the figure
            fig_path = os.path.join(output_folder, f"{site_name}_analysis.png")
            plt.savefig(fig_path)
            plt.close(fig)
            
        except Exception as e:
            print(f"Could not generate plots for {site_name}: {e}")

def classify_imbalance(
    folder_path: str,
    output_xlsx: str = "classified_sites_report.xlsx",
    normalize: bool = False,
    top_n: int = 10,
    n_clusters: int = 4,
    parallel: bool = True,
    generate_images: bool = True
):
    """
    Process multiple CSV files containing power data and generate comprehensive reports.
    
    Parameters:
    -----------
    folder_path : str
        Path to folder containing CSV files
    output_xlsx : str
        Path for output Excel report
    normalize : bool
        Whether to normalize features before clustering
    top_n : int
        Number of top problematic sites to highlight
    n_clusters : int
        Number of clusters to use in KMeans algorithm
    parallel : bool 
        Whether to use parallel processing
    generate_images : bool
        Whether to generate analysis plots for sites
        
    Returns:
    --------
    Path to saved Excel report
    """
    start_time = datetime.now()
    folder = Path(folder_path)
    csv_files = list(folder.glob("*.csv"))
    print(f"Found {len(csv_files)} CSV files to process")
    
    all_results = []
    result_dict = {}
    top_sites = []
    all_summaries = []
    
    # Process files (either in parallel or sequentially)
    if parallel and len(csv_files) > 1:
        with ProcessPoolExecutor() as executor:
            futures = {executor.submit(process_phase_csv, csv, normalize, n_clusters): csv for csv in csv_files}
            for future in futures:
                df_result, message = future.result()
                print(message)
                if df_result is not None:
                    site_name = df_result["site_name"].iloc[0]
                    result_dict[site_name] = df_result
                    all_results.append(df_result)
                    all_summaries.append(df_result.attrs["site_summary"])
                    
                    # Log top-N worst (high imbalance & high load)
                    df_top = df_result.sort_values(["category", "total_load", "imbalance"], ascending=[True, False, False])
                    top_sites.append(df_top.head(top_n))
    else:
        for csv in csv_files:
            df_result, message = process_phase_csv(csv, normalize, n_clusters)
            print(message)
            if df_result is not None:
                site_name = df_result["site_name"].iloc[0]
                result_dict[site_name] = df_result
                all_results.append(df_result)
                all_summaries.append(df_result.attrs["site_summary"])
                
                # Log top-N worst (high imbalance & high load)
                df_top = df_result.sort_values(["category", "total_load", "imbalance"], ascending=[True, False, False])
                top_sites.append(df_top.head(top_n))
    
    # If no valid results were processed, exit
    if not all_results:
        print("No valid results were processed. Check the error messages above.")
        return None
    
    # Generate Excel report
    with pd.ExcelWriter(output_xlsx, engine='xlsxwriter') as writer:
        # Create summary sheet first
        summary_df = pd.DataFrame(all_summaries)
        # Sort by avg_total_load (descending) and avg_imbalance (ascending)
        summary_df = summary_df.sort_values(["avg_total_load", "avg_imbalance"], ascending=[False, False])
        summary_df.to_excel(writer, sheet_name="Site_Summary", index=False)
        create_phase_summary_sheet(writer, all_summaries)

        # Combine all results
        combined_df = pd.concat(all_results)
        combined_df.to_excel(writer, sheet_name="All_Sites", index=False)

        # Combine worst sites from all datasets
        if top_sites:
            worst_df = pd.concat(top_sites).sort_values(["category", "total_load", "imbalance"], ascending=[True, False, False])
            worst_df = worst_df.head(top_n)
            worst_df.to_excel(writer, sheet_name="Top_Worst_Sites", index=False)

                # Add individual site sheets
        for df_result in all_results:
            site_name = df_result["site_name"].iloc[0]
            sheet_name = site_name[:31]  # Excel sheet name length limit
            df_result.to_excel(writer, sheet_name=sheet_name, index=False)
            
            # Add conditional formatting to the imbalance column
            workbook = writer.book
            worksheet = writer.sheets[sheet_name]
            
            # Get the column index for imbalance
            imb_col_idx = df_result.columns.get_loc("imbalance")
            worksheet.conditional_format(1, imb_col_idx, len(df_result), imb_col_idx, 
                                      {'type': '3_color_scale',
                                       'min_color': '#F8696B',  # Green
                                       'mid_color': '#FFEB84',  # Yellow
                                       'max_color': '#63BE7B'}) # Red

    # Generate plots if requested
    if generate_images:
        images_folder = os.path.splitext(output_xlsx)[0] + "_plots"
        generate_phase_plots(result_dict, images_folder)
        print(f"📊 Analysis plots saved to: {os.path.abspath(images_folder)}")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print(f"\n🎉 Batch classification complete! Report saved to: {os.path.abspath(output_xlsx)}")
    print(f"⏱️ Processing completed in {duration:.2f} seconds")
    print(f"📑 Processed {len(all_results)} sites successfully")
    
    return output_xlsx

# -----------------------------
# 🧪 Usage Example:
# classify_imbalance(
#     "datasets/",
#     normalize=True,
#     top_n=10,
#     parallel=True,
#     generate_images=True
# )
# -----------------------------
# -----------------------------
# 🧪 Usage Example:
# path = r'E:\_UNHCR\CODE\DATA\phase\input/'
# classify_imbalance(path, normalize=True, top_n=10, parallel=True, generate_images=True)
# # -----------------------------

pass

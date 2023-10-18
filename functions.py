import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import datetime
import os
import shutil

def extrapolate_student_plane(data, EPT_year, Stud_years, Category):
    ## Student plane interpolation
    for year in Stud_years:
        mask = (data["Year"]==year)&(data["Category"]==Category)
        nearest_year = year
        while (data.loc[(data["Year"]==nearest_year)&(data["Category"]==Category), "CO2"].isna().all()):
            nearest_year += 1
        serie = pd.Series()
        serie["Year"] = year
        ratio_evolution = ((EPT_year.loc[year]-EPT_year.loc[nearest_year])/(EPT_year.loc[nearest_year]))
        serie["CO2"] = data.loc[(data["Year"]==nearest_year)&(data["Category"]==Category)&(data["Category"]==Category), "CO2"].sum()* (1+ratio_evolution)
        serie["Value"] = data.loc[(data["Year"]==nearest_year)&(data["Category"]==Category)&(data["Category"]==Category), "Value"].sum()
        serie["Data description"] = "Estimé"
        serie["Scope"] = data.loc[(data["Year"]==nearest_year)&(data["Category"]==Category), "Scope"].median()
        serie["Campus"] = "EPFL"
        serie["Category"] = Category
        serie["Theme"] = "Voyages professionnels"
        data = data.append(serie, ignore_index=True)
    return data

def objectives_2024(data_objectives, year_reference):
    mask = (data_objectives["Category"] == "Electricite")&(data_objectives["Year"]==2019)
    Electricity_2019 = data_objectives.loc[mask].sum()
    Objective_2024_Electricity = Electricity_2019["CO2"]*0.85 #CO2 et kWh varient lineairement, le facteur de reduction est donc le meme
    Objective_2024_Electricity_value = Electricity_2019["Value"]*0.85
    Electricity_ref_year = data_objectives.loc[(data_objectives["Category"] == "Electricite")&(data_objectives["Year"]==year_reference)].sum()
    Obj_2024= np.interp([2023], [year_reference, 2024], [Electricity_ref_year["CO2"], Objective_2024_Electricity]) 
    Obj_2024_value = np.interp([2023], [year_reference, 2024], [Electricity_ref_year["Value"], Objective_2024_Electricity_value])
    
    #Append Objective 2024 to obj_2023
    df_2024 = pd.DataFrame()
    df_2024["Theme"] = ["Electricite","Electricite"]
    df_2024["Year"] = [2023, 2024]
    df_2024["Campus"] = ["EPFL", "EPFL"]
    df_2024["CO2"] =  np.append(Obj_2024, Objective_2024_Electricity)
    df_2024["Data description"] = "Objectif EPFL 15% de diminution d'ici 2024"
    df_2024["Category"] = "Electricite"
    df_2024["Scope"] = "2"
    df_2024["Unit"] = "kg CO2"
    df_2024["Value"] = np.append(Obj_2024_value, Objective_2024_Electricity_value)
    return df_2024

def Check_join_data_category(data, join_factor, ignore_column = ["Plane staff", "Plane students"], ignore_year = 2006):
    data = data[data["Year"]!=ignore_year]
    NaN_factor = join_factor[join_factor["factor name"].isna()]["Category"]
    mask_data_NaN = ~data["Category"].isin(NaN_factor)
    filtered_nan_data = data[mask_data_NaN]
    NaN_categories = data[~mask_data_NaN]["Category"].unique()
    missing_category = filtered_nan_data[~filtered_nan_data["Category"].isin(join_factor[~join_factor["factor name"].isna()]["Category"])]["Category"]
    missing_category = missing_category[~missing_category.isin(ignore_column)]
    #Print if filtered_nan_data["Category"] is not in join_factor["Category"] and reversed
    if len(missing_category.unique())>0:
        print("Missing category in join_factor")
        #Substract NaN_categories from missing_category
        missing_category = missing_category[~missing_category.isin(NaN_categories)]
        print(missing_category.unique())
        ValueError("Check coherency between join_factor and data")
        
    if len(join_factor[~join_factor["factor name"].isna()]["Category"].unique()) != len(join_factor[~join_factor["factor name"].isna()]["Category"]):
        print("Duplicate category in join_factor")
        ValueError("Check coherency between join_factor and data")

def print_missing_factor(df, emission_factor):
    #Remove None
    df = df[~df["factor name"].isna()]
    if not df["factor name"].isin(emission_factor["Data name mapping"]).all():
        print(df["factor name"][~df["factor name"].isin(emission_factor["Data name mapping"])])
        raise ValueError("Some factor name are not found in the emission factor dataframe")
    
def join_emission_factor(emission_join, factor_df, Primary_key, CO2_col="GWP [kg CO2eq]"):
    emission_factor = emission_join[Primary_key]
    name = factor_df[factor_df["Data name mapping"]==emission_factor][CO2_col].squeeze()
    if not name:
        ValueError("Could not find the data name")
    return name

def sum_variable(df, meta, field):
    meta_field = list(field.keys())[0]
    mask = np.where(meta.loc[meta_field]==field[meta_field])[0]+1
    tot = np.sum(df[mask],1)
    return tot 

def parse_df_from_ID(df_format, L0_ID_elec):
    df_elec_L0 = df_format[L0_ID_elec]
    df_elec_L0.columns = df_elec_L0.iloc[0]
    df_elec_L0 = df_elec_L0[1:]
    df_elec_L0=df_elec_L0.replace("-", np.nan)
    return df_elec_L0


def export_history(year, emission_factor_path, factor_join, df):
    date = str(datetime.now().year) + "_"+str(datetime.now().month)+"_"+str(datetime.now().day)
    export_folder = "data/Level1/"+str(year)+"/"+date
    if date not in os.listdir("data/Level1/"+str(year)):
        os.makedirs(export_folder)
    shutil.copy(factor_join, export_folder+"/factor_join.json")
    
    #shutil.copy(emission_factor_path, export_folder+"/emission_factor_db.xlsx")
    df.to_csv(export_folder+f"/bilan_{year}.csv")

def join_factor_variable(df, factor, join_df):
    df_emission = pd.DataFrame(np.nan, index=df.index, columns=df.columns)
    for ID in df.columns:
        if "factor_name" in join_df[str(ID)]:
            for elem in join_df[str(ID)]["factor_name"]:
                date_range = list(elem.values())[0]
                factor_name = list(elem.keys())[0]
                for date in date_range:
                    idx_factor=np.where(factor["Data name mapping"] == factor_name)[0]
                    if not idx_factor:
                        print(f"Could not find the factor {factor_name}")
                    emission_factor = factor["GWP [kg CO2eq]"].iloc[idx_factor]
                    df_emission[ID][int(date)] = df.loc[int(date)]*emission_factor 
        else:
            idx_factor=np.where(factor["Data name mapping"] == join_df[str(ID)])[0]
            emission = factor["GWP [kg CO2eq]"].iloc[idx_factor]
            df_emission[ID] = emission*df[ID]
    return df_emission
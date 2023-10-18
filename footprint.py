import pandas as pd
import numpy as np
from functions import *

class footprint():
    def __init__(self, CO2_col, EPT_infile):
        self.general_attributes = {
            "author": "Hugo Cruz",
            "author_mail": "hugo.cruz@epfl.ch",
        }
        self.CO2_col = CO2_col
        self.bilan = pd.DataFrame()
        self.data = pd.DataFrame()
        self.category_name = {}
        self.Theme_name = {}
        self.ignore_column = []
        #open EPT file, this EPT is not up to date, the evolution is not yet taken into account
        EPT = pd.read_excel(EPT_infile)
        self.EPT_year = EPT[EPT["Category"]== 'Population GRI'].groupby("Year").sum()["EPT"]

    def read_emission_factor(self, emission_factor_path):
        if "google" in emission_factor_path:
            factor = pd.read_csv(emission_factor_path, encoding= 'cp1252', delimiter=';', decimal=",")
        else:
            factor = pd.read_excel(emission_factor_path, index_col=0)
        CO2_col = self.CO2_col
        #Standardize unit: MWh, m3, km
        factor_df = factor.copy()
        
        mask_kWh = factor_df["Unit"]=="kWh"
        factor_df[CO2_col] = factor_df[CO2_col].where(~mask_kWh,factor_df[CO2_col]*1000)
        factor_df["Unit"] = factor_df["Unit"].replace("kWh", "MWh")

        mask_MJ = factor_df["Unit"]=="MJ"
        factor_df[CO2_col] = factor_df[CO2_col].where(~mask_MJ,factor_df[CO2_col]*3600)
        factor_df["Unit"] = factor_df["Unit"].replace("MJ", "MWh")

        mask_m = factor_df["Unit"]=="m"
        factor_df[CO2_col] = factor_df[CO2_col].where(~mask_m,factor_df[CO2_col]/1000)
        factor_df["Unit"] = factor_df["Unit"].replace("m", "km")
        self.emission_factor = factor_df

    def run_energy_emission(self, elec_path):
        df = pd.read_excel(elec_path)
        df = df.rename(columns={"year": "Year", "campus": "Campus", "category": "Category", "value": "Value"})
        df["Theme"] = df["Category"] #We understand that the theme is Energy, however it's more relevant to separate when we visualize it
        df["Scope"] = 2
        df["Unit"] = df["unit"]
        df["Path"] = elec_path
        df.drop("unit", axis=1, inplace=True)

        mask_mazout =  df["Category"]=="Mazout"
        mask_gaz = df["Category"]=="Gaz"
        df.loc[df["Category"]=="Electricite","Category"] = "Electricite"

        df.loc[mask_gaz|mask_mazout, "Scope"] = 1

        #ADD FRIBOURG ELECTRICITE BECAUSE NOT IN GRI
        Fribourg_electricite = (36003.81+ 206631.15)/1000  #MWh
        #Add to df Fribourg_eletricite
        df = df.append({"Category": "Electricite", "Year": 2022, "Campus": "Fribourg", "Value": Fribourg_electricite, "Theme": "Electricite", "Scope": 2, "Unit": "MWh", "Path": "Bilan CO2 code"}, ignore_index=True)   

        self.data = pd.concat([self.data, df], ignore_index=True)

    def run_dechets_emission(self, dechets_path):
        #Dechets is not yet taken into account
        df = pd.read_excel(dechets_path)
        df = df.rename(columns={"waste_type":"Category", "kg":"Value", "year": "Year"})
        df["Scope"] = 3
        df["Unit"] = "kg"
        df["Campus"] = "Vaud"
        df["Theme"] = "Dechets"
        df["ID_category"] = df["Category"]
        #Strategy ojective 2025
        mask = (df["disposal_method"]=="Recycling")&(df["Year"]==2022)
        recycled = df[(df["disposal_method"]=="Recycling")&(df["Year"]==2022)]["Value"].sum()
        incineration = df[(df["disposal_method"]=="incineration")&(df["Year"]==2022)]["Value"].sum()
        total = recycled+incineration
        recycled_2025 = total*0.8
        incineration_2025 = total*0.2
        df["Path"] = dechets_path
        df.drop("disposal_method", axis=1, inplace=True)
        print("In 2025, the amount of recycled waste should be {} kg based on 80% recycled in 2022".format(recycled_2025))
        
        self.data = pd.concat([self.data, df], ignore_index=True)

    def run_plane_emission(self,path):
        staff_emission = pd.read_excel(path, sheet_name="Avions (staff)", skiprows=1)
        staff_CO2 = staff_emission.set_index("Data*").loc["CO2 footprint (tons) / Year        Données: Atmosfair"]
        staff_CO2.index = staff_CO2.index.astype(int)
        staff_CO2 = staff_CO2.astype(float)
        Students_emission = pd.read_excel(path, sheet_name="Avions (étudiant-e-s)")
        Students_group = Students_emission.groupby("Year").sum()
        Students_CO2 = Students_group["CO2 RFI2,7"]
        
        CO2_staff = staff_CO2*1000 #Convert t to kg
        CO2_students = Students_CO2*1000 #Convert t to kg
        df_stud = pd.DataFrame(CO2_students).reset_index()
        df_staff = pd.DataFrame(CO2_staff).reset_index()
        self.category_name["Avion etudiant-e-s"] = "Avion etudiant-e-s"
        self.category_name["Avion collaborateur-trice-s"] = "Avion collaborateur-trice-s"
        df_staff["Category"] =  self.category_name["Avion collaborateur-trice-s"]
        df_stud["Category"] = self.category_name["Avion etudiant-e-s"]
        df_staff.columns = ["Year", "CO2", "Category"]
        df_stud.columns = ["Year", "CO2", "Category"]

        df = pd.concat([df_stud, df_staff], ignore_index=True)
        df["Campus"] = "EPFL"
        df["Theme"] = "Voyages professionnels"
        df["Unit"] = "kg CO2e"
        df["Scope"] = 3
        df["Path"] = path
        self.ignore_column = np.append(self.ignore_column, df["Category"].unique())
        self.data = pd.concat([self.data, df], ignore_index=True)

    def run_train_emission(self, input_file):
        #Open and format the data
        df_train = pd.read_excel(input_file,skiprows=1, sheet_name="Train")
        df_train = df_train.set_index("Km").stack().reset_index()
        df_train.columns=["Category", "Year", "Value"]
        #Select only interesting data 
        df = df_train[(df_train["Category"]=="Total Domestique")|(df_train["Category"]=="Total International")]

        df["Year"] = df["Year"].astype(int)
        df["Value"] = df["Value"].astype(float)
        df["Unit"] = "km"
        df["Theme"] = "Voyages professionnels"
        df["Scope"] = 3
        df["Campus"] = "EPFL"
        df["Path"] = input_file
        self.data = pd.concat([self.data, df], ignore_index=True)

    def run_cars_emission(self, input_file):
        df_cars = pd.read_excel(input_file, skiprows=1, sheet_name="Voiture")

        V_service = df_cars.iloc[0:5]
        Mobility = df_cars.iloc[9:12]

        Mobility.columns = df_cars.iloc[8]
        col_int = list(Mobility.columns[1:].astype(str).str.replace(" ", "").str.split(".").str[0])
        #Set Mobility.columns[1:] to col_int
        Mobility.columns = ["Litres / kW pour électriques"] + col_int
        db_Mobility = Mobility.set_index("Litres / kW pour électriques").stack().reset_index()
        db_Mobility.columns=["Category", "Year", "Value"]
        db_Mobility["Value"] = db_Mobility["Value"].astype(str).str.replace("-", "NaN").str.split(".").str[0].astype(float)
        db_Mobility["Year"] = db_Mobility["Year"].astype(int)
        db_Mobility["Unit"] = "L"

        db_service = V_service.set_index("Kilomètres parcourus").stack().reset_index()
        db_service.columns=["Category", "Year", "Value"]
        db_service["Year"] = db_service["Year"].astype(int)
        db_service["Value"] = db_service["Value"].astype(float)
        db_service["Unit"] = "km"

        V_service_L = df_cars.iloc[15:17]
        V_service_L.columns = df_cars.iloc[14]
        col_int = list(V_service_L.columns[1:].astype(str).str.replace(" ", "").str.split(".").str[0])
        #Set Mobility.columns[1:] to col_int
        V_service_L.columns = ["Litres"] + col_int
        db_service_L = V_service_L.set_index("Litres").stack().reset_index()
        db_service_L.columns=["Category", "Year", "Value"]
        db_service_L["Year"] = db_service_L["Year"].astype(int)
        db_service_L["Value"] = db_service_L["Value"].astype(float)
        db_service_L["Unit"] = "L"
        #Merge db_service db_Mobility and db_service_L into new dataframe
        #Avoid a double count by selecting only the volume of L used in service vehicles:
        db_service = db_service[~db_service["Category"].isin(["Véhicules de service essence", "Véhicules de service diesel", "Mobility carsharing"])]
        db_Mobility = db_Mobility[~db_Mobility["Category"].isin(["Mobility électriques"])] #L'Electricite est achetée par l'EPFL et redistribuée, elle est donc deja comptabilisée 
        df = pd.concat([db_service, db_Mobility, db_service_L], ignore_index=True)

        df["Campus"] = "EPFL"
        df["Theme"] = "Voyages professionnels"
        df["Scope"] = 3
        df.loc[df["Category"]=="Véhicules de service essence", "Scope"] = 1
        df.loc[df["Category"]=="Véhicules de service diesel", "Scope"] = 1
        df.loc[df["Category"]=="Mobility diesel", "Scope"] = 1
        df.loc[df["Category"]=="Mobility essence", "Scope"] = 1
        df["Path"] = input_file

        self.data = pd.concat([self.data, df], ignore_index=True)

    def run_mobilite_EPFL(self, pendulair_final):
        df = pd.read_excel(pendulair_final, skiprows=2, index_col="Unnamed: 0")
        #Select columns to keep in dataframe
        col_dist=[]
        for col in df.columns:
            if "km" in col:
                col_dist.append(col)

        df_dist = df.iloc[:7][col_dist].T.replace("-", np.nan)

        #if an index contain "jour" drop it
        for idx in df_dist.index:
            if "jour" in idx:
                df_dist = df_dist.drop(idx)
            if "Hiver" in idx:
                #Replace "hiver" in index by ""
                df_dist.loc[idx] = df_dist.loc[idx.replace("Hiver", "Ete")]+df_dist.loc[idx]
                #Rename idx by idx.replace("Hiver", "")
                df_dist = df_dist.rename(index={idx:idx.replace("Hiver", "")})
                #Drop idx.replace("Hiver", "Ete")
                df_dist = df_dist.drop(idx.replace("Hiver", "Ete"))
        
        df_dist.index = df_dist.index.str.extract("(\d+)").squeeze()
        #if index==np.nan drop it
        df_dist = df_dist.loc[~df_dist.index.isna()]
        df_dist = df_dist.reset_index().rename(columns={0:"Year"})
        #Columns of df_dist are transport mode, convert df_dist to database
        db = df_dist.set_index("Year").stack().reset_index()
        db.rename(columns={"level_1":"Category", 0:"Value"}, inplace=True)
        #Convert index to int
        db["Year"] = db["Year"].astype(int)
        db["Theme"] = "Mobilité pendulaire"
        db["Scope"] = 3
        db["Unit"] = "km"
        db["Campus"] = "EPFL"

        #Filter Category=="autres"
        db = db[db["Category"]!="autres"]
        #Remove from db db["Year"] below 2018. Before 2018, the data collection has changed and is not comparable (3 order of magnitude lower)
        self.raw_pendular = db.copy()
        db.loc[db["Year"]<2018, "Value"] = np.nan
        db["Path"] = pendulair_final
        self.data = pd.concat([self.data, db], ignore_index=True)

    def run_numeric(self, numeric_infile):
        df_manual_input = pd.read_excel(numeric_infile)
        df_numeric = df_manual_input[~df_manual_input["Numérique"].isna()]
        df = pd.DataFrame()
        df["CO2"] = df_numeric["Numérique"] * 1000 #Convert to kg
        df["Year"] = df_numeric["Year"]
        df["Campus"] = "EPFL"
        self.numeric_name = "Numérique (fabrication)"
        df["Category"] = self.numeric_name
        df["Theme"] = "Numérique (fabrication)"
        df["Scope"] = 3
        df["Path"] = numeric_infile
        self.ignore_column = np.append(self.ignore_column,df["Category"].unique())
        self.data = pd.concat([self.data, df], ignore_index=True)

    def run_alimentation(self, alimentation_infile):
        df = pd.read_excel(alimentation_infile) #Read file of computed CO2 by quantis
        df["CO2"] = df["CO2"]*1000 #Convert to kg
        df["Campus"] = "EPFL"
        df["Theme"] = "Alimentation"
        df["Scope"] = 3
        df["Path"] = alimentation_infile

        #Filter all values before 2020 because the methodology has changed and extrapolation of 2019 seems incorrect:
        df = df[df["Year"]>=2020]
        #append self.ignore_column with df["Category"].unique()
        self.ignore_column = np.append(self.ignore_column,df["Category"].unique())

        self.data = pd.concat([self.data, df], ignore_index=True)
        
    def run_achats(self, achat_infile):
        df_manual_input = pd.read_excel(achat_infile)
        df_achats = df_manual_input[~df_manual_input["Achats"].isna()]
        df = pd.DataFrame()
        df["CO2"] = df_achats["Achats"]*1000 #Convert to kg
        df["Year"] = df_achats["Year"]
        df["Campus"] = "EPFL"
        self.achats_name = "Achats (estimation)"
        df["Category"] = self.achats_name
        df["Theme"] = "Achats (estimation)"
        df["Scope"] = 3
        df["Path"]= achat_infile
        self.ignore_column = np.append(self.ignore_column,df["Category"].unique())
        self.data = pd.concat([self.data, df], ignore_index=True)

    def add_2006_emission(self, path_2006):
        self.Scope1_categories = ["Mazout", "Gaz", "CAD"]
        self.Scope2_categories = ["Electricite"]
        df = pd.read_excel(path_2006)
        df = df[df["Year"]==2006]
        #Set columns of df as new variable and the variables of df as values
        df = df.set_index("Year").stack().reset_index()
        #Set names of columns as ["Year", "Theme", "CO2"]
        df.columns = ["Year", "Theme", "CO2"]
        df["CO2"] = df["CO2"]*1000 #Convert to kg
        #Assign scope based on Scope1_categories and Scope2_categories
        df.loc[df["Theme"].isin(self.Scope1_categories), "Scope"] = 1
        df.loc[df["Theme"].isin(self.Scope2_categories), "Scope"] = 2
        df.loc[~df["Theme"].isin(self.Scope1_categories+self.Scope2_categories), "Scope"] = 3

        df["Campus"] = "EPFL"
        df["Year"]=2006
        df["Category"] = df["Theme"]
        #Drop Theme=="Alimentation" car il est compté dans la database Quantis
        df = df[df["Theme"]!="Alimentation"]
        df["Path"] = path_2006
        self.ignore_column = np.append(self.ignore_column, df["Category"].unique())
        self.data = pd.concat([self.data, df], ignore_index=True)
    
    def export_database(self, path, Level="L1"):
        if Level=="L1":
            self.data.to_excel(path, index=False)
        if Level=="L2":
            self.data_L2.to_excel(path, index=False)
        if Level=="L3A":
            self.data_L3A.to_excel(path, index=False)
        if Level=="L3B":
            self.data_L3B.to_excel(path, index=False)

    def import_database(self, path):
        self.data = pd.read_excel(path)
        
    def assign_factor(self, join_factor_path):
        #Assigne un facteur d'emission à chacune des lignes de la database self.data
        #Join_factor_path: path to the file containing the factor to join to the database
        #Aucun calcul n'est fait, juste une assignation de facteur. Les calculs sont fait dans la fonction calculate_emission 
        join_factor = pd.read_excel(join_factor_path)
        Check_join_data_category(self.data, join_factor, ignore_column=self.ignore_column, ignore_year=2006)
        
        self.data["Year"] = self.data["Year"].astype(int)
        self.data["Value"] = self.data["Value"].astype(float)
        self.data["Data description"] = "Valeur calculée"
        
        df = self.data
        
        df["factor name"] = np.nan
        # First assign factor to Year
        mask_year = ~join_factor["Year"].isna()
        df_year = join_factor.loc[mask_year]
        for year, campus, category, factor in df_year[["Year", "Campus", "Category", "factor name"]].values:
            mask = (df["Year"]==year) & (df["Campus"]==campus) & (df["Category"]==category) & df["factor name"].isna()
            df.loc[mask, "factor name"] = factor

        # If no Year specified, assign factor to Campus
        mask_campus = join_factor["Year"].isna() & ~join_factor["Campus"].isna()
        df_campus = join_factor.loc[mask_campus].set_index(["Campus", "Category"])
        for campus, category in df_campus.index.unique():
            if (mask := (df["Campus"]==campus) & (df["Category"]==category) & df["factor name"].isna()).any():
                factor = df_campus.loc[(campus, category)]["factor name"]
                df.loc[mask, "factor name"] = factor

        # If no year and no campus, assign factor to Category
        mask_category = join_factor["Year"].isna() & join_factor["Campus"].isna()
        df_category = join_factor.loc[mask_category].set_index("Category")["factor name"]
        for category in df["Category"].unique():
            if (mask := (df["Category"]==category) & df["factor name"].isna()).any():
                factor = df_category.get(category)
                #If factor have multiple value, assign the first one
                if isinstance(factor, pd.Series):
                    factor = factor.iloc[0]
                df.loc[mask, "factor name"] = factor
        self.data = df.copy()
        #Print missing factor, les facteurs non trouvés 
        print_missing_factor(self.data, self.emission_factor)

    def calculate_emission(self):
        #Calcule les emissions liées à chaque ligne de la database self.data
        #Mask les valeurs n'ayant pas de facteur d'emission (ex: avions dont la donnée de base n'est pas utilisée mais on importe directement le CO2)
        mask_na_factor = self.data["factor name"].isna()|~self.data["CO2"].isna()
        data_L2 = self.data[~mask_na_factor].join(self.emission_factor.set_index("Data name mapping"), on="factor name", rsuffix="_CO2", how="left")
        
        #Complete les données maskées par des données brutes de CO2
        data_L2["CO2"] = data_L2["Value"]*data_L2[self.CO2_col]
	
        mask_brute_CO2 = ~self.data["CO2"].isna()
        data_L2 = pd.concat([data_L2, self.data[mask_brute_CO2]], ignore_index=True)
        #Drop unecessary columns
        data_L2.drop(columns =["Utilisation", "Comments", "Lien source"], inplace=True)
        self.data_L2 = data_L2.copy()

        ## Add new column called "Equivalence string" and "Equivalence value"
        # Create mask of "Category"=="Electricite" and fill with "Equivalence string"="km en train Suisse"
        Train_suisse = 1.6 #kg CO2/aller-retour zurich en train 

        mask_electricity = self.data_L2["Category"]=="Electricite"


    def extrapolate(self):
        #Si une valeur est manquante pour une année, l'algorithme cherche une valeur dans le future et l'adapte selon la croissance
        #Si c'est une valeur dans le future n'est pas disponible, alors elle va chercher dans le passé et adpate selon croissance
        data = self.data_L2.copy()
        years = data["Year"].unique()
        years.sort()
        years = years[:-1]

        EPT_year = self.EPT_year 

        ## Student plane interpolation
        Category = self.category_name["Avion etudiant-e-s"]
        Stud_years = [2016,2017, 2018]
        data = extrapolate_student_plane(data, EPT_year, Stud_years, Category)

        for year in years:
            for Theme in data["Theme"].unique():
                mask = (data["Year"]==year)&(data["Theme"]==Theme)
                #For each Theme where there is no data in this year, take the data from the nearest year
                if data.loc[mask, "CO2"].isna().all():
                    #Find the nearest year where data["Theme"] is not empty
                    nearest_year = year
                    while (data.loc[(data["Year"]==nearest_year)&(data["Theme"]==Theme), "CO2"].isna().all()):
                        nearest_year += 1
                        if nearest_year>2100:
                            nearest_year = year
                            while (data.loc[(data["Year"]==nearest_year)&(data["Theme"]==Theme), "CO2"].isna().all()):
                                nearest_year -= 1
                                

                    nearest_mask = (data["Year"]==nearest_year)&(data["Theme"]==Theme)
                    data_masked = data.loc[nearest_mask]

                    for category in data_masked["Category"].unique():
                        serie = pd.Series()
                        serie["Year"] = year
                        serie["Theme"] = Theme
                        serie["Category"] = category
                        if Theme == "Electricite":
                            print(1)
                        if Theme == "Mobilité pendulaire":
                            #Produit en croix pour estimer la valeur de la mobilité pendulaire en fonction de l'évolution de la population
                            data_pendular = data.loc[(data["Year"]==nearest_year)&(data["Theme"]=="Mobilité pendulaire")]
                            pendular_year = self.raw_pendular.loc[(self.raw_pendular["Year"] == year)&(self.raw_pendular["Theme"]=="Mobilité pendulaire")&(self.raw_pendular["Category"]==category)]
                            pendular_year["Part_modale"] = pendular_year["Value"]/pendular_year["Value"].sum()
                            pendular_nearest = self.raw_pendular.loc[(self.raw_pendular["Year"] == nearest_year)&(self.raw_pendular["Theme"]=="Mobilité pendulaire")&(self.raw_pendular["Category"]==category)]
                            pendular_nearest["Part_modale"] = pendular_nearest["Value"]/pendular_nearest["Value"].sum()

                            mask = data_pendular["Category"]==category
                            pendular_EPT_a = pendular_year[pendular_year["Category"]==category]["Part_modale"].values*EPT_year[year]
                            pendular_CO2_n = data_pendular[mask]["CO2"].values
                            pendular_EPT_n = pendular_nearest[pendular_nearest["Category"]==category]["Part_modale"].values*EPT_year[nearest_year]
                            #Produit en croix:
                            serie["CO2"] = (pendular_EPT_a*pendular_CO2_n/pendular_EPT_n)[0]   
                        else:
                            ratio_evolution = ((EPT_year.loc[year]-EPT_year.loc[nearest_year])/(EPT_year.loc[nearest_year]))
                            serie["CO2"] = data.loc[(data["Year"]==nearest_year)&(data["Theme"]==Theme)&(data["Category"]==category), "CO2"].sum()* (1+ratio_evolution)
                        serie["Value"] = data.loc[(data["Year"]==nearest_year)&(data["Theme"]==Theme)&(data["Category"]==category), "Value"].sum()*(1+ratio_evolution)
                        serie["Data description"] = "Estimé"
                        serie["Scope"] = data.loc[(data["Year"]==nearest_year)&(data["Theme"]==Theme), "Scope"].median()
                        serie["Campus"] = "EPFL"
                        serie["Category"] = Theme
                        data = data.append(serie, ignore_index=True)

        self.data_L3A = data

    def objectives(self, year_reference=2022):
        data_L3A = self.data_L3A.copy()

        #Objectif 2024
        self.df_objectifs_2024 = objectives_2024(data_L3A, year_reference)
        self.year_reference = year_reference
        #Objectif 2030
        df = pd.DataFrame()
        df["Theme"] = data_L3A["Theme"].unique()
        df["Year"] = 2030
        df["Campus"] = "EPFL"
        df["Scope"] = np.nan
        #Definition des objectifs 2030:
        mask = (data_L3A["Theme"] == "Electricite")|(data_L3A["Theme"] == "Mazout")|(data_L3A["Theme"]=="CAD")|(data_L3A["Theme"] == "Gaz")
        Energy_2006 = data_L3A.loc[(data_L3A["Year"]==2006)&(mask)].sum()
        Objective_2030_energy = Energy_2006["CO2"]*0.5
        gaz_2030 = data_L3A.loc[(data_L3A["Year"]==2006)&(data_L3A["Theme"] == "Gaz")]["CO2"]*0.5 #Assume que le gaz en 2030 est la moitié de 2006
        elec_2030 = Objective_2030_energy-gaz_2030
        df.loc[df["Theme"]=="Electricite", "CO2"] = elec_2030.squeeze()
        df.loc[df["Theme"]=="Electricite", "Objectif"] = "-50% energie par rapport à 2006"
        df.loc[df["Theme"]=="Gaz", "CO2"] = gaz_2030.squeeze()
        df.loc[df["Theme"]=="Gaz", "Objectif"] = "-50% energie par rapport à 2006"        
        df.loc[df["Theme"]=="Mazout", "CO2"] = 0 
        df.loc[df["Theme"]=="Mazout", "Objectif"] = "Remplacement par Electricite"
        df.loc[df["Theme"]=="CAD", "CO2"] = 0
        df.loc[df["Theme"]=="CAD", "Objectif"] = "Remplacement par Electricite"

        mask_voyages_prof = (data_L3A["Theme"]=="Voyages professionnels")
        trip_2019 = data_L3A.loc[(data_L3A["Year"]==2019)&(mask_voyages_prof)]["CO2"].sum()
        Objective_2030_trip = trip_2019*0.7
        df.loc[df["Theme"]=="Voyages professionnels", "CO2"] = Objective_2030_trip
        df.loc[df["Theme"]=="Voyages professionnels", "Objectif"] = "-30% par rapport à 2019"

        Pendulaire_2019 =  data_L3A.loc[(data_L3A["Year"]==2019)&(data_L3A["Theme"]=="Mobilité pendulaire")]["CO2"].sum()
        Objective_2030_pendulaire = Pendulaire_2019*0.7
        df.loc[df["Theme"]=="Mobilité pendulaire", "CO2"] = Objective_2030_pendulaire
        df.loc[df["Theme"]=="Mobilité pendulaire", "Objectif"] = "-30% par rapport à 2019"

        Alimentation_2019 = data_L3A.loc[(data_L3A["Year"]==2019)&(data_L3A["Theme"]=="Alimentation")]["CO2"].sum()
        Objective_2030_alimentation = Alimentation_2019*0.6
        df.loc[df["Theme"]=="Alimentation", "CO2"] = Objective_2030_alimentation
        df.loc[df["Theme"]=="Alimentation", "Objectif"] = "-40% par rapport à 2019"

        df.loc[df["Theme"]=="Numérique (fabrication)", "CO2"] = 4829e3
        df.loc[df["Theme"]=="Numérique (fabrication)", "Objectif"] = "Malgré la croissance, maintenir l'impact à 2019"

        
        df.loc[df["Theme"]==self.achats_name, "CO2"] = 42000e3
        df.loc[df["Theme"]==self.achats_name, "Objectif"] = "Malgré la croissance, maintenir l'impact à 2019"

        df.loc[df["Theme"]=="Dechets", "CO2"] = data_L3A.loc[(data_L3A["Year"]==2019)&(data_L3A["Theme"]=="Dechets")]["CO2"].sum()

        df["Data description"] = "Objectifs 2030 (confédération+EPFL)"
        df["Category"] = df["Theme"]

        for Theme in df["Theme"].unique():
            df.loc[df["Theme"]==Theme, "Scope"] = data_L3A[data_L3A["Theme"] == Theme]["Scope"].iloc[0]

        self.df_2030 = df

    def interpolation_objectifs(self):

        data_L3A = self.data_L3A.copy()
        df_2030 = self.df_2030

        years = np.arange(data_L3A["Year"].max(),2030)
        for Theme in df_2030["Theme"].unique():
            for year in years:
                df_temp = pd.Series()
                df_temp["Theme"] = Theme
                df_temp["Year"] = year
                df_temp["Campus"] = "EPFL"
                df_temp["Category"] = df_temp["Theme"]
                df_temp["Data description"] = "Objectifs 2030 (confédération+EPFL)"
                df_temp["Scope"] = data_L3A[data_L3A["Theme"] == Theme]["Scope"].iloc[0]
                mask = (data_L3A["Year"]==data_L3A["Year"].max())&(data_L3A["Theme"]==Theme)

                if mask.sum():
                    df_temp["CO2"] = np.interp(year, [data_L3A["Year"].max(), 2030], [data_L3A.loc[mask, "CO2"].sum(), df_2030.loc[(df_2030["Theme"]==Theme)&(df_2030["Year"]==2030), "CO2"].sum()])
                    
                    if year != data_L3A["Year"].max():
                        df_2030 = df_2030.append(df_temp, ignore_index=True)
                else:
                    #If this data is not available for a specific theme, we take the value of the last year
                    df_temp["CO2"] = np.interp(year, [data_L3A["Year"].max(), 2030], [data_L3A.loc[(data_L3A["Year"]==data_L3A["Year"].max()-1)&(data_L3A["Theme"]==Theme), "CO2"].iloc[0], df_2030.loc[(df_2030["Theme"]==Theme)&(df_2030["Year"]==2030), "CO2"].sum()])
                    df_2030 = df_2030.append(df_temp, ignore_index=True)
                
        data_L3B =  pd.concat([data_L3A, df_2030, self.df_objectifs_2024],  ignore_index=True)
        Elect_emission_factor = data_L3B[(data_L3B["Theme"]=="Electricite")&(data_L3B["Year"]==self.year_reference)][self.CO2_col].mean()
        nan_mask = (data_L3B["Value"].isna())&(data_L3B["Category"] == "Electricite")

        data_L3B.loc[nan_mask, "Value"] = data_L3B[nan_mask]["CO2"].iloc[0]/Elect_emission_factor
        self.data_L3B = data_L3B.copy()

    def simulation():
        #Ajoute les reformes et leurs impacts sur le bilan CO2 et les predictions: Exemple: politique voyage
        
        #Dans le cas de la simple croissance, des simulations peuvent être faites.

        print(1)
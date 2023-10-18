import yaml
from footprint import *

year_reference = 2022
local_run = True

with open("scripts/input_path.yaml", "r", encoding='utf8') as f:
    directories = yaml.load(f, Loader=yaml.FullLoader)

fp = footprint(CO2_col='Factor [kg CO2eq]', EPT_infile=directories["EPT_infile"])

if ~local_run:
    fp.run_energy_emission(directories["energy_infile"])
    fp.run_cars_emission(directories["reporting_voyages"])
    fp.run_train_emission(directories["reporting_voyages"])
    fp.run_plane_emission(directories["reporting_voyages"])
    fp.run_mobilite_EPFL(directories["reporting_pendulaire"])
    #fp.run_dechets_emission(directories["dechets_infile"])
    fp.run_alimentation(directories["alimentation_infile"])
    fp.run_achats(directories["Manual_input"])
    fp.run_numeric(directories["Manual_input"])
    fp.add_2006_emission(directories["Manual_input"])
    fp.export_database(directories["L1_data"], Level="L1")
else:
    fp.import_database(directories["L1_data"], Level="L1")

fp.read_emission_factor(directories["emission_factor"])
fp.assign_factor(directories["factor_join"])
fp.calculate_emission()
fp.export_database(directories["L2_data"], Level="L2")
fp.extrapolate()
fp.export_database(directories["L3A_data"], Level="L3A")
fp.objectives(year_reference=year_reference)
fp.interpolation_objectifs()
fp.export_database(directories["L3B_data"], Level="L3B")

#fp.simulations()
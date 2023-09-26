import pandas as pd
import numpy as np

input_valid_keys=['OPID', 'ASA', 'Sex', 'Age', 'HT', 'WT', 'BMI', 'rdw', 'mpv', 'pdw',
       'e_alc', 'e_neutro', 'na', 'alp', 'tc', 'egfr_ce', 'egfr_md',
       'icteric_index', 'wbc', 'rbc', 'mcv', 'mch', 'mchc', 'e_lympho',
       'e_baso', 'ig', 'plt', 'hb', 'hct', 'pt_sec', 'pt_inr', 'aptt', 'k',
       'cl', 'ca', 'glu', 'cr', 'bun', 'ast', 'alt', 'tp', 'ua', 'tco2',
       'bilirubin', 'crp', 'alb', 'mono', 'e_anc', 'OP_CODE', 'EMOP',
       'induc_propofol', 'induc_midazolam', 'induc_pentotal', 'Desflurane',
       'Sevoflurane', 'Propofol', 'WARD_SBP', 'WARD_DBP', 'WARD_MBP',
       'Pre-induction_SBP', 'Pre-induction_DBP', 'Pre-induction_MBP',
       'Hypotension', 'Hypotension_Area', 'Hypotension_Time(minute)', 'MBP_Max', 'MBP_Mean', 'MBP_Min',
       'area_time']
path = "/srv/project_data/EMR/jy/Post-induction/Merge_data/"
input_data = pd.read_excel(path+"20230926_Post-induction_input.xlsx")
output_data = pd.read_excel(path+"20230925_outcomes_55.xlsx")
old_output_data = pd.read_csv(path+"temp_outcomes_csv_ver.csv")

output_data.rename(columns={'anes_num':'OPID', 'Hypotension':'H55', 'Hypotension_Area':'H55_Area', 'Hypotension_Time(minute)':'H55_Time(M)', 'MBP_Max':'H55_Max', 'MBP_Mean':'H55_Mean', 'MBP_Min':'H55_Min'}, inplace=True)
old_output_data.rename(columns={'anes_num':'OPID', 'Hypotension':'H60', 'Hypotension_Area':'H60_Area', 'Hypotension_Time(minute)':'H60_Time(M)', 'MBP_Max':'H60_Max', 'MBP_Mean':'H60_Mean', 'MBP_Min':'H60_Min'}, inplace=True)

PIH_data = pd.merge(input_data, output_data, how='left', on=['OPID'])
PIH_data = pd.merge(PIH_data, old_output_data, how='left', on=['OPID'])

PIH_data.to_csv(path+"20230926_PIH.csv", index=False, encoding="utf-8-sig")
PIH_data.to_excel(path+"20230926_PIH.xlsx", index=False)

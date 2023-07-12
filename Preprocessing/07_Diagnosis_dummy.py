import pandas as pd
import numpy as np

preprocessing_path = 'C:/Users/user/Dropbox/김진영_SignalHouse/0_마취기록지/Post-induction/input_preprocessing/'
original_path = 'C:/Users/user/Dropbox/김진영_SignalHouse/0_마취기록지/original_data/'

diagnosis = pd.read_csv(preprocessing_path+'02_Diagnosis_split.csv')

test = pd.get_dummies(diagnosis, columns=['IDC'])
test.to_csv(preprocessing_path+'07_Diagnosis_dummies.csv', index=False, encoding='utf-8-sig')
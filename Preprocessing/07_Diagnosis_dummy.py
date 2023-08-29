import pandas as pd
import numpy as np

preprocessing_path = '/srv/project_data/EMR/jy/Post-induction/Input_preprocessing/'
original_path = '/srv/project_data/EMR/original_data/'

diagnosis = pd.read_csv(preprocessing_path+'02_Diagnosis_split.csv')

test = pd.get_dummies(diagnosis, columns=['IDC'])
# test.to_csv(preprocessing_path+'07_Diagnosis_dummies.csv', index=False, encoding='utf-8-sig')

len(test['마취기록작성번호'].unique())

test[test['마취기록작성번호'] == 20190700021805]

aa = test.groupby('마취기록작성번호')
bb = aa.max().reset_index()

bb.to_csv(preprocessing_path+"07_Diagnosis_dummies.csv", index=False, encoding='utf-8-sig')


#### dummies test
for data in bb['마취기록작성번호'].unique():
    for key in bb.keys():
        if key == '마취기록작성번호':
            temp_key = bb[bb[key] == data]
            continue
        tdd = temp_key[key].to_numpy()[0]
        if tdd != 0:
            print(data, temp_key[key])


import pandas as pd

middle_pat = pd.read_excel('/srv/project_data/EMR/jy/Post-induction/Input_preprocessing/middle_pat.xlsx')
diagnosis = pd.read_excel('/srv/project_data/EMR/original_data/22_1019_ANS_이상욱_2021-1352_(진단)_4차(1.3).xlsx', sheet_name=None)

def EMR_column_arrange(pat_info):
    pat_info.columns = pat_info.loc[0]
    pat_info = pat_info.drop([0])
    pat_info = pat_info.reset_index(drop=True)
    return pat_info

pat_diag1 = diagnosis['1'].copy()
pat_diag2 = diagnosis['2'].copy()
pat_diag3 = diagnosis['3'].copy()

pat_diag1 = EMR_column_arrange(pat_diag1)
pat_diag2 = EMR_column_arrange(pat_diag2)
pat_diag3 = EMR_column_arrange(pat_diag3)

pat_diag = pd.concat([pat_diag1, pat_diag2, pat_diag3]).reset_index(drop=True)

pat_diag['IDC'] = pat_diag['통합진단코드'].str[:4]

pat_diag_merge = pat_diag[['마취기록작성번호', 'IDC']]
pat_diag_merge['마취기록작성번호'] = pat_diag_merge['마취기록작성번호'].astype(int)

middle_diag_pat = pd.merge(middle_pat, pat_diag_merge, how='inner', on=['마취기록작성번호'])

pat_diag_merge.to_csv('/srv/project_data/EMR/jy/Post-induction/Input_preprocessing/cut_diag_4word.csv', index=False, encoding='utf-8-sig')

print(len(pat_diag_merge['IDC'].unique()))
pat_diag_merge = pat_diag_merge.drop_duplicates(['마취기록작성번호', 'IDC'])
test = []
testap = test.append
for i in pat_diag_merge['마취기록작성번호'].unique():
    testap(len(pat_diag_merge[pat_diag_merge['마취기록작성번호'] == i]))
    if len(pat_diag_merge[pat_diag_merge['마취기록작성번호'] == i]) == 61:
        print(i)

for i in pat_diag_merge[pat_diag_merge['마취기록작성번호'] == 20201000105961].iloc():
    print(i)

print(pat_diag_merge[pat_diag_merge['마취기록작성번호'] == 20201000105961])

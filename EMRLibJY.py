import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

def read_patient_info():
    test1 = pd.read_excel('/srv/project_data/EMR/original_data/211015_ANS_이상욱_2021-1352_1차(3.14).xlsx', sheet_name=None)
    test2 = pd.read_excel('/srv/project_data/EMR/original_data/221017_ANS_이상욱_2021-1352_(EMR)_1차(11.2).xlsx',
                          sheet_name=None)

    test1['대상환자&1'].rename(columns=test1['대상환자&1'].iloc[0], inplace=True)
    test1['대상환자&1'].drop([0], inplace=True)
    test1['대상환자&1'].reset_index(drop=True, inplace=True)

    test2['0&1'].rename(columns=test2['0&1'].iloc[0], inplace=True)
    test2['0&1'].drop([0], inplace=True)
    test2['0&1'].reset_index(drop=True, inplace=True)

    return test1, test2

def read_PIH_patient_info():
    patient_df = pd.read_excel("/srv/project_data/EMR/jy/Post-induction/Input_preprocessing/old/merge_emr20230712.xlsx")
    del patient_df['Unnamed: 0']

    return patient_df

def read_surgery_info():
    anes_19 = pd.read_excel('/srv/project_data/EMR/original_data/20220426_ANS_이상욱_2021-1352_(1.마취기본)_3차(7.7).xlsx')
    anes_20 = pd.read_excel('/srv/project_data/EMR/original_data/22_1018_ANS_이상욱_2021-1352_(1.마취기본)_2차(12.22).xlsx')

    anes_19.columns = anes_19.iloc[0]
    anes_19 = anes_19.drop([0]).reset_index(drop=True)
    anes_20.columns = anes_20.keys().str.replace("'", "")

    anes_20.rename(columns={'(마취기록)마취소요시간(분)': '(마취기록) 마취소요시간(분)'}, inplace=True)
    anes_20.rename(columns={'(마취기록) ASA점수코드': '(마취기록) ASA점수'}, inplace=True)
    anes_20 = anes_20.drop(columns=['(마취기록) 최종확인일시', '성별'])

    anes = pd.concat([anes_19, anes_20])

    anes['마취기록작성번호'] = anes['마취기록작성번호'].astype(int)
    anes['(마취기록)마취시작'] = pd.to_datetime(anes['(마취기록)마취시작'], format='%Y-%m-%d %H:%M')
    anes['(마취기록)수술시작'] = pd.to_datetime(anes['(마취기록)수술시작'], format='%Y-%m-%d %H:%M')

    anes.drop_duplicates('마취기록작성번호', inplace=True)
    anes.reset_index(drop=True, inplace=True)

    return anes

def read_intra_operation_data():
    path = "/srv/project_data/EMR/jy/Post-induction/Outcomes_preprocessing/monitering.npz"
    aa = np.load(path, allow_pickle=True)
    column_names = ['RN', '병원등록번호', '마취기록작성번호', '수술일자', '수술일정번호', '모니터링명', '모니터링약어명', '모니터링기록일시', '측정값']
    data_array = aa['arr_0']
    df = pd.DataFrame(data_array, columns=column_names)

    return df

def cleaning_abp(df):
    df['마취기록작성번호'] = df['마취기록작성번호'].astype(int)
    bp_df = df[df['모니터링약어명'].str.contains('BP')]
    bp_df['모니터링기록일시'] = pd.to_datetime(bp_df['모니터링기록일시'])
    bp_df['측정값'] = bp_df['측정값'].astype(float)
    abp_df = df[df['모니터링약어명'].str.contains('ABP')]
    nibp_df = df[df['모니터링약어명'].str.contains('ABP') == False]

    nisbp_df = nibp_df[nibp_df['모니터링약어명'].str.contains('S')]
    nisbp_df = nisbp_df.drop_duplicates(['마취기록작성번호', '모니터링기록일시'])
    nidbp_df = nibp_df[nibp_df['모니터링약어명'].str.contains('D')]
    nidbp_df = nidbp_df.drop_duplicates(['마취기록작성번호', '모니터링기록일시'])
    nimbp_df = nibp_df[nibp_df['모니터링약어명'].str.contains('M')]
    nimbp_df = nimbp_df.drop_duplicates(['마취기록작성번호', '모니터링기록일시'])

    nibp_df = pd.merge(nisbp_df, nidbp_df, how='inner', on=['마취기록작성번호', '모니터링기록일시'])
    nibp_df.rename(columns={'측정값_x': 'sbp', '측정값_y': 'dbp'}, inplace=True)
    nibp_df = pd.merge(nibp_df, nimbp_df, how='inner', on=['마취기록작성번호', '모니터링기록일시'])
    nibp_df.rename(columns={'측정값': 'mbp'}, inplace=True)

    nibp_df = nibp_df[['마취기록작성번호', '모니터링기록일시', 'sbp', 'mbp', 'dbp']]

    sbp_df = abp_df[abp_df['모니터링약어명'].str.contains('S')]
    sbp_df = sbp_df.drop_duplicates(['마취기록작성번호', '모니터링기록일시'])
    dbp_df = abp_df[abp_df['모니터링약어명'].str.contains('D')]
    dbp_df = dbp_df.drop_duplicates(['마취기록작성번호', '모니터링기록일시'])
    mbp_df = abp_df[abp_df['모니터링약어명'].str.contains('M')]
    mbp_df = mbp_df.drop_duplicates(['마취기록작성번호', '모니터링기록일시'])

    abp_df = pd.merge(sbp_df, dbp_df, how='inner', on=['마취기록작성번호', '모니터링기록일시'])
    abp_df.rename(columns={'측정값_x': 'sbp', '측정값_y': 'dbp'}, inplace=True)
    abp_df = pd.merge(abp_df, mbp_df, how='inner', on=['마취기록작성번호', '모니터링기록일시'])
    abp_df.rename(columns={'측정값': 'mbp'}, inplace=True)

    abp_df = abp_df[['마취기록작성번호', '모니터링기록일시', 'sbp', 'mbp', 'dbp']]
    abp_df = pd.concat([nibp_df, abp_df])
    abp_df.drop(abp_df[pd.isnull(abp_df['sbp'])].index, inplace=True)
    abp_df.drop(abp_df[abp_df['sbp'].str.contains('^[a-zA-Z]*$', None) == True].index, inplace=True)
    abp_df = abp_df[abp_df['sbp'].str.contains('^[0-9\.]*$', None) == True]
    abp_df = abp_df[abp_df['dbp'].str.contains('^[0-9\.]*$', None) == True]
    abp_df = abp_df[abp_df['mbp'].str.contains('^[0-9\.]*$', None) == True]
    abp_df.drop(abp_df[abp_df['sbp'] == '.'].index, inplace=True)
    abp_df.drop(abp_df[abp_df['mbp'] == '.'].index, inplace=True)
    abp_df.drop(abp_df[abp_df['mbp'] == '5..6'].index, inplace=True)
    abp_df = abp_df.reset_index(drop=True)
    abp_df['sbp'] = abp_df['sbp'].astype(float)
    abp_df['dbp'] = abp_df['dbp'].astype(float)
    abp_df['mbp'] = abp_df['mbp'].astype(float)

    return abp_df

def plot_signal(data, time, type='plot', bins=40):
    sns.set_style(
        {
            "axes.edgecolor": "0",
            "ytick.color": "0",
            "xtick.color": "black",
            "xtick.direction": "in",
            "ytick.direction": "in",
            "text.color": "black",
            "axes.spines.right": False,
            "axes.spines.top": False,
            "axes.facecolor": "white",
            "axes.grid": False,
            "patch.edgecolor": "black",
            'font.family': ['sans-serif']
        }
    )
    plt.figure(figsize=(12,8))
    if type == 'plot':
        plt.plot(time, data, 'b', linewidth=2)
    elif type == 'hist':
        plt.hist(data, bins)
    # plt.axhline(60, color='red', label="Hypotension threshold")
    # plt.ylim([20, 140])
    plt.xticks(fontsize=16)
    plt.yticks(fontsize=16)
    plt.show()
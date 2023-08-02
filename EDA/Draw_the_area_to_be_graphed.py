import numpy as np
import pandas as pd
import dask.dataframe as dd
import polars as pl
import scipy.integrate as sci

anes_19 = pd.read_excel('/srv/project_data/EMR/original_data/20220426_ANS_이상욱_2021-1352_(1.마취기본)_3차(7.7).xlsx')
anes_20 = pd.read_excel('/srv/project_data/EMR/original_data/22_1018_ANS_이상욱_2021-1352_(1.마취기본)_2차(12.22).xlsx')

anes_19.columns = anes_19.iloc[0]
anes_19 = anes_19.drop([0]).reset_index(drop=True)
anes_20.columns = anes_20.keys().str.replace("'", "")

anes_20.rename(columns= {'(마취기록)마취소요시간(분)':'(마취기록) 마취소요시간(분)'}, inplace=True)
anes_20.rename(columns= {'(마취기록) ASA점수코드':'(마취기록) ASA점수'}, inplace=True)
anes_20 = anes_20.drop(columns=['(마취기록) 최종확인일시', '성별'])

anes = pd.concat([anes_19, anes_20])

anes['마취기록작성번호'] = anes['마취기록작성번호'].astype(int)
anes['(마취기록)마취시작'] = pd.to_datetime(anes['(마취기록)마취시작'], format = 'mixed')
anes['(마취기록)수술시작'] = pd.to_datetime(anes['(마취기록)수술시작'], format = 'mixed')

path = "/srv/project_data/EMR/jy/Post-induction/Outcomes_preprocessing/monitering.npz"
aa = np.load(path, allow_pickle=True)
column_names = ['RN', '병원등록번호', '마취기록작성번호', '수술일자', '수술일정번호', '모니터링명', '모니터링약어명', '모니터링기록일시', '측정값']
data_array = aa['arr_0']
df = pd.DataFrame(data_array, columns=column_names)
dd_df = dd.from_array(data_array, columns = column_names)
pl_df = pl.from_numpy(data = data_array, schema= column_names)

df_bp = df[df['모니터링약어명'].str.contains('BP')].copy()
df_bp = df_bp[['마취기록작성번호', '모니터링약어명', '모니터링기록일시', '측정값']]
df_bp['측정값'] = df_bp['측정값'].astype(float)
df_bp['모니터링기록일시'] = pd.to_datetime(df_bp['모니터링기록일시'])
pl_df = pl.from_pandas(df_bp)
df_mbp = df_bp[df_bp['모니터링약어명'].str.contains('M')]

for anes_num, anes_time, surgery_time in zip(anes['마취기록작성번호'], anes['(마취기록)마취시작'], anes['(마취기록)수술시작']):
    pat_data = df_mbp[(df_mbp['마취기록작성번호'] == anes_num) & (df_mbp['모니터링기록일시'] >= anes_time) & (df_mbp['모니터링기록일시'] <= surgery_time)].reset_index(drop=True)
    if any(pat_data['측정값'] < 60) == False:
        print(f"{anes_num} 저혈압 없음")
        continue
    pat_data['minute'] = ((pat_data['모니터링기록일시'] - pat_data['모니터링기록일시'].min()).dt.total_seconds() / 60)
    pat_data['minute'] = pat_data['minute'].astype(int)
    break

###
import sys
sys.getsizeof(df)
sys.getsizeof(dd_df)
sys.getsizeof(pl_df)


from matplotlib.patches import Polygon
import matplotlib.pyplot as plt

from scipy import interpolate
t = pat_data['minute'].to_numpy()
v = pat_data['측정값'].to_numpy()
dt = np.append([5], np.diff(t))
f = interpolate.interp1d(t, v, kind='linear')
total_time = t[-1]
area_dt = 0.1
tnew = np.arange(total_time / area_dt + 1) * area_dt
vnew = f(tnew)
ind = np.where(vnew < 60)[0]
hvnew = vnew[ind]

# plt.style.use('seaborn-v0_8-whitegrid')
# mpl.rcParams['font.family'] = 'serif'

import seaborn as sns

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
sns.set_palette("Set2")

fig, ax = plt.subplots(figsize=(12,8))
plt.plot(tnew,vnew,'b',linewidth=2, label="ABP_M")
# plt.ylim([30,170])
plt.axhline(60, color='red', label="Hypotension threshold")
plt.axvline(tnew.min(), color='black', linestyle='dashed')
plt.axvline(tnew.max(), color='black', linestyle='dashed')
plt.xlim([-2,72])
plt.ylim([40,100])
plt.xticks(fontsize=16)
plt.yticks(fontsize=16)

a = pat_data['측정값'].min()
b = pat_data['측정값'].max()
# verts = [(60,0)]+list(zip(pat_data['minute'],pat_data['측정값']))+[(b,60)]
verts = list(zip(tnew[ind], vnew[ind]))
verts[0] = (verts[0][0], 60)
poly = Polygon(verts, facecolor='0.7', edgecolor='0.5', label='Hypotension area')
ax.add_patch(poly)
plt.xlabel("Minute of surgery", size=20, weight=500, labelpad=3)
plt.ylabel("Mean Blood Pressure (mmHg)", size=20, weight=500, labelpad=3)
plt.legend(fontsize=12, bbox_to_anchor = (0.95, 1))
plt.annotate('Induction',
            xy=(18.5, -17), xycoords='axes points',
            xytext=(0, -40), textcoords='offset points',
            arrowprops=dict(facecolor='black', shrink=0.05),
            fontsize=16)
plt.annotate('Incision',
            xy=(652, -17), xycoords='axes points',
            xytext=(0, -40), textcoords='offset points',
            arrowprops=dict(facecolor='black', shrink=0.05),
            fontsize=16)
# plt.savefig('/srv/project_data/EMR/jy/Post-induction/fig1.png', bbox_inches='tight', dpi=300)
fig.savefig('/srv/project_data/EMR/jy/Post-induction/fig1.png', bbox_inches='tight', dpi=300)

##############################
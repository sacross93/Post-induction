import pandas as pd
import glob
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import EMRLibJY

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
sns.dark_palette("#69d", reverse=True, as_cmap=True)
# sns.color_palette("light:b", as_cmap=True)
sns.color_palette("vlag", as_cmap=True)
sns.color_palette("coolwarm", as_cmap=True)

addr = '/srv/project_data/EMR/original_data/'
sr_list = glob.glob(addr+"*1.마취기본*.xlsx")

op_df = EMRLibJY.read_intra_operation_data()
surgery_df = EMRLibJY.read_surgery_info()
patient_df = EMRLibJY.read_PIH_patient_info()

path = "/srv/project_data/EMR/jy/Post-induction/Outcomes_preprocessing/monitering.npz"
aa = np.load(path, allow_pickle=True)
column_names = ['RN', '병원등록번호', '마취기록작성번호', '수술일자', '수술일정번호', '모니터링명', '모니터링약어명', '모니터링기록일시', '측정값']
data_array = aa['arr_0']
df = pd.DataFrame(data_array, columns=column_names)
df['마취기록작성번호'] = df['마취기록작성번호'].astype(int)
bp_df = df[df['모니터링약어명'].str.contains('BP')]

#참고 논문: Intraoperative Hypotension Is Associated With Adverse Clinical Outcomes After Noncardiac Surgery
import pandas as pd
import numpy as np

path = "/srv/project_data/EMR/jy/Post-induction/Outcomes_preprocessing/monitering.npz"
aa = np.load(path, allow_pickle=True)
column_names = ['RN', '병원등록번호', '마취기록작성번호', '수술일자', '수술일정번호', '모니터링명', '모니터링약어명', '모니터링기록일시', '측정값']
data_array = aa['arr_0']
df = pd.DataFrame(data_array, columns=column_names)

test = df[df['모니터링약어명'].str.contains('ABP')]
test['모니터링기록일시'] = pd.to_datetime(test['모니터링기록일시'])



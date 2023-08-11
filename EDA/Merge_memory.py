import polars as pl
import pandas as pd

data_path = '/srv/project_data/EMR/jy/Post-induction/Input_preprocessing/'

pldf = pl.read_csv(data_path+'07_Diagnosis_dummies.csv')
## Polars의 Lazy evaluation을 이용하여 실제 메모리를 사용하지 않고 읽음

current_data_types = pldf.dtypes
current_data_names = pldf.columns

new_data_type = pl.Int8

new_df = pl.DataFrame({col: pldf[col].cast(new_data_type) for col in pldf.columns if col != '마취기록작성번호'})
new_df = new_df.with_columns(pldf['마취기록작성번호'])

pddf = new_df.to_pandas()

import sys
sys.getsizeof(pddf)
## Pandas의 기본 Dtype은 Int64이다. Int64 --> Int8로 바꾸면서 3970MB --> 496MB만 사용하도록 함

input_data = pd.read_excel(data_path+'Post-induction_input.xlsx')
sys.getsizeof(input_data)
## 메모리 388MB 사용

merge = pd.merge(input_data, pddf, how='left', on='마취기록작성번호')

sys.getsizeof(merge)
## 메모리 757MB 사용

## 총 1,641MB 필요


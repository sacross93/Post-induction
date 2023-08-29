import os
import tensorflow as tf
import tensorflow.keras as keras
from tensorflow.keras import backend as K
import pandas as pd
import numpy as np
from tableone import TableOne
from sklearn.metrics import confusion_matrix
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
import xgboost as xgb
from sklearn.metrics import precision_recall_curve
from sklearn.metrics import average_precision_score
import missingno as msno
# RF
from sklearn.ensemble import RandomForestClassifier
# LR
from sklearn.linear_model import LogisticRegression
import tensorflow as tf
from tensorflow.keras import layers
import tensorflow.keras as keras
from keras.models import Sequential
from keras.callbacks import EarlyStopping, ModelCheckpoint
from keras.layers import Dense, Activation, Dropout
from keras import backend as K
from scipy.stats import zscore
import sklearn.utils as utils
from sklearn.metrics import roc_curve
from sklearn.metrics import roc_auc_score
import pickle
from IPython.display import display, HTML
from tqdm.notebook import tqdm
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier
import sklearn.svm as svm
from sklearn.multiclass import OneVsRestClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.metrics import confusion_matrix
import sklearn.svm as svm
from sklearn.metrics import accuracy_score, recall_score, precision_score, f1_score
import scipy.stats as stats
from scipy.stats import zscore
import polars as pl
pd.options.display.max_columns = None

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

input_data = pd.read_csv(data_path+'PIH_input_new.csv')
sys.getsizeof(input_data)
## 메모리 388MB 사용

# merge = pd.merge(input_data, pddf, how='left', on='마취기록작성번호')

old_input = pd.read_excel(data_path+'Post-induction_input.xlsx')
fixed_pat_info = pd.read_excel(data_path+'exception_weight_height.xlsx')


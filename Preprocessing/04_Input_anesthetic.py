import sys
sys.path
import pandas as pd
import EMR_Preprocessing as emr

method_anesthesia1 = pd.read_excel('/srv/project_data/EMR/original_data/20220426_ANS_이상욱_2021-1352_(3.서식_마취방법,인계사항)_4차(7.12).xlsx')
method_anesthesia2 = pd.read_excel('/srv/project_data/EMR/original_data/22_1018_ANS_이상욱_2021-1352_(3.서식_마취방법,인계사항)_2차(12.22).xlsx')
middle_pat = pd.read_excel('/srv/project_data/EMR/jy/Post-induction/Input_preprocessing/middle_pat.xlsx')

emr.column_arrange()

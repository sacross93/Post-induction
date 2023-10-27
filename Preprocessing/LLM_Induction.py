import bardapi
import os
os.environ['_BARD_API_KEY'] = "cgjXrc1lROaGb9f66Sn6goUFBlaDc-Fv5h5Qhuye491knvynNj_u5Xl5CAaunMacgJBlQQ."
import pandas as pd

event1 = pd.read_excel('/srv/project_data/EMR/original_data/20220426_ANS_이상욱_2021-1352_(4.EVENT)_5차(7.15).xlsx')
event2 = pd.read_excel('/srv/project_data/EMR/original_data/22_1018_ANS_이상욱_2021-1352_(4.Event)_2차(12.22).xlsx')

del event1['수술일자']
del event2['마취기록이벤트순서']

event2 = event2.rename(columns={'마취기록이벤트기록시간': '마취기록이벤트기록일시'})

event_info = pd.concat([event1, event2]).reset_index(drop=True)


input_text = "내가 질문을 해도 될까?"

response = bardapi.core.Bard().get_answer(input_text)
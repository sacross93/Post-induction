from bardapi import Bard, BardCookies
from bardapi.constants import SESSION_HEADERS
import os
import pandas as pd
import requests

event1 = pd.read_excel('/srv/project_data/EMR/original_data/20220426_ANS_이상욱_2021-1352_(4.EVENT)_5차(7.15).xlsx')
event2 = pd.read_excel('/srv/project_data/EMR/original_data/22_1018_ANS_이상욱_2021-1352_(4.Event)_2차(12.22).xlsx')

del event1['수술일자']
del event2['마취기록이벤트순서']

event2 = event2.rename(columns={'마취기록이벤트기록시간': '마취기록이벤트기록일시'})

event_info = pd.concat([event1, event2]).reset_index(drop=True)

### BARD 테스트
os.environ['_BARD_API_KEY'] = "cgjXrfU0OBZIu7BMxJxbZGDKbMLa8-9qu-zN01RrG_XX2Egw776Twefui96gKxJlMbyIMg."
session = requests.Session()
session.headers = {
            "Host": "bard.google.com",
            "X-Same-Domain": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
            "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
            "Origin": "https://bard.google.com",
            "Referer": "https://bard.google.com/",
        }
session.cookies.set("__Secure-1PSID", os.getenv("_BARD_API_KEY"))
token = "cgjXrQeWYQYLSnMr_qxnY6Gds2Do3P-ioydML1qqhXN54I1RyB2lvWYGt8O1gZcDUtXO7Q."
bard = Bard(token=token, session=session, timeout=9999999)
cookie_dict = {
    "__Secure-1PSID": "cgjXrfkz_3rlFHyWvtpB3h413cxzAAZP5IjPTK4mxFWtQCsct2pqVR15BlfGyh4c7vwcVA.",
    "__Secure-1PSIDTS": "sidts-CjEBNiGH7veoz_60qPwcn1VHljgYlowXvodbWvZSIiLuh27VBl_q4TspvNGdzbivuPRiEAA",
    "__Secure-1PSIDCC": "ACA-OxNgFAKFjLhX2zIEMz6gdQO5kdcCe_uQHJhcriezXct7XQv1CfCn6ySIfAG3ZcvZkcGcWA"
}
bard = BardCookies(cookie_dict=cookie_dict)


input_text = """
    수술 이벤트 기록지를 통하여 마취 유도제와 마취 유지제를 구별하려고 해.
    마취 유도제로 사용되었다면 이름과 용량을 알고싶고, 마취 유지제로 사용되었다면 이름만 알면 돼.
    마취 유도제로 사용되는 약물 종류로는 propofol, midazolam, pentotal이 있어.
    마취 유지제로는 desflurane, sevoflurane, propofol이 있어.
    약물 종류는 줄임말을 사용할 수 있어. 예를들어 propofol은 pofol, desflurane은 des, sevoflurane은 sev 등 줄여서 말하는 경우가 있어.
    propofol은 줄임말 뿐 아니라 다르게 부르는 경우가 있어 freefol, fresofol이라고 부르기도 해.
    propofol은 마취 유도제와 마취 유지제 둘 다 사용되는데, 구별하는 방법은 propofol의 용량이 500mg 미만으로 사용되었다면 마취 유도제로 사용한거야.
    예를들어 propofol 80mg, 2% propofol 1000mg라고 수술 이벤트 기록지에 적혀있다면, 마취 유도제로 propofol 80mg, 마취 유지제로 2% propofol 1000mg을 사용한거야.
    예를들어 propofol 20+150 mg이라고 되어 있다면 마취유도제로 propofol을 170mg을 사용한거야.
    앞으로 내가 수술 이벤트 기록지를 보여주면 위의 작업을 해줘.
    """

# response = bardapi.core.Bard().get_answer(input_text)
response = bard.get_answer(input_text)
print(response['content'])

liter_list = event_info['마취기록작성번호'].unique()

for idx, anes_num in enumerate(liter_list):
    temp_info = event_info[event_info['마취기록작성번호'] == anes_num]
    if idx > 3:
        break

input_text = str(temp_info['마취기록이벤트내용'])
response = bard.get_answer(input_text)
print(response['content'])

temp_info['마취기록이벤트내용'].iloc[2]
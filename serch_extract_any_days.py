"""파일을 수정하여 돌릴때 따로 저장을 하여 돌려주세요
    원하는 날짜 있으면 밑의 if 문의 범위를 변경을 해주세요
    이 파일의 기본 예시는 16일부터 20일을 추출하는 코드입니다"""

import pandas as pd
import os
from tqdm import tqdm
from functools import partial  # tqdm progressbar 오류 잡아주는 코드
from collections import defaultdict
tqdm = partial(tqdm, position = 0, leave = True )

def search_date(traj, date, days):
    """해당 날짜 찾아 날짜별로 리스트 생성 및 업로드 함수"""

    tmp_date = date // 100000000
    day = tmp_date - 171200
    if day >= 11 and day < 12:
        days[day].append(traj)
    return days

def dict_to_dataframe(x, Days):
    """dictionary를 데이터프레임 형식으로 바꿔주는 함수"""
    for i in tqdm(range(31), desc = '데이터프레임 변환'):
        Days[i] = pd.DataFrame(x[i])
    return Days

if __name__ == "__main__":
    # 활용 데이터 디렉토리
    # 본인이 이용할 데이터 폴더 위치 입력
    dtg_dir = r'\\192.168.0.246\stl_200301\2. 연구용역\2021_(국토부) 교통플랫폼 기반 신규 교통서비스\DTG\DTG_new_16\UTMK_16\\'
    os.chdir(dtg_dir)
    file_list = os.listdir(dtg_dir)

    day_list = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']

    

    # 폴더내 파일 불러오기
    for file in tqdm(file_list, desc = '전체파일리스트'):
        filename, fileExt = os.path.splitext(file)

    # csv 파일 원하는 개수만큼 이용하여 추출하기
    # default는 range(0,400)으로 DTG 데이터 전체 400개임
    # 폴더는 없더라도 일일이 생성하지 않아도 되므로 걱정하지 않아도 됨
    # 0~199까지는 1일부터 15일까지, 200~399는 16일부터 31일 데이터가 존재함
    # 이에 빈 리스트일 때 1kb 파일을 생성하지 않도록 해주는 코드가 마지막 4줄임
    if '.csv' in fileExt:
        for file_num in tqdm(range(99, 100), desc = '파일개수'):
            df = pd.read_csv(dtg_dir + filename[:5] + str(file_num) + '_UTMK' + fileExt, encoding = 'utf-8')
            
            # 빈 리스트 한번에 생성하기
            # days = [[] for _ in range(31)]
            days = defaultdict(list)
            Days = ['D' + str(i) for i in range(171201, 171232)]
            
            for i in tqdm(range(len(df)), desc = '한파일라인수'):

                df_traj = df.iloc[i]
                df_date = df.iloc[i,7]

                search_date(df_traj, df_date, days)

            dict_to_dataframe(days, Days)

            for date in tqdm(range(1, 32), desc = '1712'):
                save_path = 'F:\\DTG_new_16\\1712' + day_list[date-1]
                if not os.path.isdir(save_path):
                    os.mkdir(save_path)
                if not days[date]:
                    continue
                else:
                    Days[date].to_csv(save_path + '\\2017-' + str(file_num) + ".csv", index = False)

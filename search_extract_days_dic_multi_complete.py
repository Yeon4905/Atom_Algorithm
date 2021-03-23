"""배포판 v.0
    주의사항
    1. 사용할 때는 꼭 따로 저장후에 사용하기를 바랍니다.
    2. 개인에 맞게 수정이 필요한 부분은 위와 같이 (""" """) 내에 표시하였으니 꼭 확인 후 실행하도록 합니다"""

import pandas as pd
import os
import parmap
from tqdm import tqdm
from functools import partial  # tqdm progressbar 오류 잡아주는 코드
from collections import defaultdict
tqdm = partial(tqdm, position = 0, leave = True )


def search_date(traj, date, days):
    """해당 날짜 찾아 날짜별로 리스트 생성 및 업로드 함수"""
    tmp_date = date // 100000000
    day = tmp_date - 171200
    """ 아래는 원하는 날짜 추출 시 이"""
    # if day >= 16 and day < 21:
    days[day].append(traj)
    return days

def dict_to_dataframe(x, Days):
    """dictionary를 데이터프레임 형식으로 바꿔주는 함수"""
    for i in range(32):
        Days[i] = pd.DataFrame(x[i])
    return Days


def do_it_all( file_lst, dtg_dir, filename, fileExt, day_list, days, Days):

    for file_num in tqdm(file_lst, desc = '파일개수'):
        df = pd.read_csv(dtg_dir + filename[:5] + str(file_num) + '_UTMK' + fileExt, encoding = 'utf-8')

        for i in range(len(df)):
            df_traj = df.iloc[i]
            df_date = df.iloc[i,7]
            search_date(df_traj, df_date, days)
        Days = dict_to_dataframe(days, Days)

        for date in range(1, 32):
            """ 저장공간 변경 필요"""
            save_path = 'C:\\Users\\HYU\\Desktop\\PR\\PR_New1\\1712' + day_list[date-1]

            if not os.path.isdir(save_path):
                os.mkdir(save_path)
            if not days[date]:
                continue
            else:
                Days[date].to_csv(save_path + '\\2017_' + str(file_num) + ".csv", index = False)
    return


if __name__ == "__main__":
    """활용 데이터 디렉토리
    본인이 이용할 데이터 폴더 위치 입력"""
    dtg_dir = r'\\192.168.0.246\stl_200301\2. 연구용역\2021_(국토부) 교통플랫폼 기반 신규 교통서비스\DTG\DTG_new_16\UTMK_16\\'
    os.chdir(dtg_dir)
    file_list = os.listdir(dtg_dir)

    day_list = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']

    # 빈 리스트 한번에 생성하기
    days = defaultdict(list)
    Days = ['D' + str(i) for i in range(171200, 171232)]

    # 폴더내 파일 불러오기
    for file in tqdm(file_list, desc = '전체파일리스트'):
        filename, fileExt = os.path.splitext(file)

    """ 1. 병렬로 이용할 수 있는 cpu 개수는 컴퓨터마다 다르니 확인 후 설정해주세요.
        2. 전부다 이용하면 컴퓨터가 과부하되니 꼭 알맞게 설정해주세요.
        3. 현재 파일은 총 400개입니다.
        4. 아래의 예시는 4개로 나누었을때입니다.
        그러므로, file_lst와  num_cores를 알맞게 변경해주세요
        ex. num_cores=3일때, 400을 적절하게 3개로 나누어 세범위표현"""

    file_lst = [range(0,100), range(100, 200), range(200,300), range(300,400)]
    num_cores = 4
    if '.csv' in fileExt:
        parmap.map(do_it_all, file_lst, dtg_dir, filename, fileExt, day_list, days, Days,
                    pm_pbar = True, pm_processes = num_cores)

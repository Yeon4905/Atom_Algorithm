import pandas as pd
import os
import parmap
from tqdm import tqdm
from functools import partial  # tqdm progressbar 오류 잡아주는 코드
from collections import defaultdict
tqdm = partial(tqdm, position = 0, leave = True )



def do_it_all( file_lst, dtg_dir, filename, fileExt, day_list, days, Days):

    for file_num in tqdm(file_lst, desc = '파일개수'):
        df = pd.read_csv(dtg_dir + filename[:5] + str(file_num) + '_UTMK' + fileExt, encoding = 'utf-8')
        """test용이라 십만줄만"""
        df = df[:100000]

        """divide the dataframe to list by number of pm_processes
            didn't need to
        # df_len = len(df)
        # num_cores = 2 """

        for i in tqdm(range(len(df)), desc = '한파일라인수'):
            df_traj = df.iloc[i]
            df_date = df.iloc[i,7]
            search_date(df_traj, df_date, days)
        Days = dict_to_dataframe(days, Days)
        for date in tqdm(range(1, 32), desc = '1712'):
            save_path = 'C:\\Users\\HYU\\Desktop\\PR\\PR_New1\\1712' + day_list[date-1]
            if not os.path.isdir(save_path):
                os.mkdir(save_path)
            if not days[date]:
                continue
            else:
                Days[date].to_csv(save_path + '\\2017_' + str(file_num) + ".csv", index = False)
    return

"""tqdm의 desc가 없을때 결과값을 위의 do_it_all과 비교함
    별 차이 없음"""
def do_it_all_at_once( file_lst, dtg_dir, filename, fileExt, day_list, days, Days):

    for file_num in file_lst:
        df = pd.read_csv(dtg_dir + filename[:5] + str(file_num) + '_UTMK' + fileExt, encoding = 'utf-8')
        df = df[:100000]
        """divide the dataframe to list by number of pm_processes"""
        # df_len = len(df)
        # num_cores = 2
        for i in range(len(df)):
            df_traj = df.iloc[i]
            df_date = df.iloc[i,7]
            search_date(df_traj, df_date, days)
        Days = dict_to_dataframe(days, Days)
        for date in range(1, 32):
            save_path = 'C:\\Users\\HYU\\Desktop\\PR\\PR_New1\\1712' + day_list[date-1]
            if not os.path.isdir(save_path):
                os.mkdir(save_path)
            if not days[date]:
                continue
            else:
                Days[date].to_csv(save_path + '\\2017_' + str(file_num) + ".csv", index = False)
    return

def search_date_to_df(df, days, Days):
    for i in tqdm(range(len(df)), desc = '한파일라인수'):
        df_traj = df.iloc[i]
        df_date = df.iloc[i,7]
        search_date(df_traj, df_date, days)
    Days = dict_to_dataframe(days, Days)

    # for date in tqdm(range(1, 32), desc = '1712'):
    #     save_path = 'C:\\Users\\HYU\\Desktop\\PR\\PR_New1\\1712' + day_list[date-1]
    #     if not os.path.isdir(save_path):
    #         os.mkdir(save_path)
    #     if not days[date]:
    #         continue
    #     else:
    #         Days[date].to_csv(save_path + '\\2017_' + str(file_num) + ".csv", index = False)
    return

def search_date(traj, date, days):
    """해당 날짜 찾아 날짜별로 리스트 생성 및 업로드 함수"""
    tmp_date = date // 100000000
    day = tmp_date - 171200
    # if day >= 16 and day < 21:
    days[day].append(traj)
    return days

def dict_to_dataframe(x, Days):
    """dictionary를 데이터프레임 형식으로 바꿔주는 함수"""
    for i in tqdm(range(32), desc = '데이터프레임 변환'):
        Days[i] = pd.DataFrame(x[i])
    return Days

def all_search_date_to_df(df, days, Days):
    for i in tqdm(range(len(df)), desc = '한파일라인수'):
        df_traj = df.iloc[i]
        df_date = df.iloc[i,7]
        tmp_date = df_date // 100000000
        day = tmp_date - 171200
        # if day >= 16 and day < 21:
        days[day].append(df_traj)
    for i in tqdm(range(32), desc = '데이터프레임 변환'):
        Days[i] = pd.DataFrame(days[i])
    return

if __name__ == "__main__":
    """꼭 쓰시오"""
    # 활용 데이터 디렉토리
    # 본인이 이용할 데이터 폴더 위치 입력
    dtg_dir = r'\\192.168.0.246\stl_200301\2. 연구용역\2021_(국토부) 교통플랫폼 기반 신규 교통서비스\DTG\DTG_new_16\UTMK_16\\'
    os.chdir(dtg_dir)
    file_list = os.listdir(dtg_dir)

    day_list = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']

    # 빈 리스트 한번에 생성하기
    # days = [[] for _ in range(31)]
    days = defaultdict(list)
    Days = ['D' + str(i) for i in range(171200, 171232)]

    # 폴더내 파일 불러오기
    for file in tqdm(file_list, desc = '전체파일리스트'):
        filename, fileExt = os.path.splitext(file)
    """기존엔 df를 n개로 나누어 돌리려고 생각하였으나 무한루프가 도는 문제가 생김
        이에 df를 나누는 것이 아닌 전체 파일을 n개로 나눌 생각을 하여 실행함
        그것이 해결책이라는 것을 알게됨"""

    file_lst = [range(0,1), range(1, 2), range(2, 3), range(3, 4)]
    if '.csv' in fileExt:
        parmap.map(do_it_all_at_once, file_lst, dtg_dir, filename, fileExt, day_list, days, Days, pm_pbar = True, pm_processes = 4)




        # for file_num in tqdm(range(200, 201), desc = '파일개수'):
        #     df = pd.read_csv(dtg_dir + filename[:5] + str(file_num) + '_UTMK' + fileExt, encoding = 'utf-8')
        #     df = df[:100000]
        #     """divide the dataframe to list by number of pm_processes"""
        #     df_len = len(df)
        #     num_cores = 2
        #     # manager = multiprocessing.Manager()
        #     df_div_list = [df[: (df_len//2)], df[(df_len//2):]]

        #     # Days = parmap.map(search_date_to_df, df_div_list, days, Days, pm_processes = n)
        #     # Days = np.concatenate(Days)
        #     # parmap.map(all_search_date_to_df, df_div_list, days, Days)
        #     search_date_to_df(df_div_list[0], days, Days)
        # for date in tqdm(range(1, 32), desc = '1712'):
        #     save_path = 'C:\\Users\\HYU\\Desktop\\PR\\PR_New1\\1712' + day_list[date-1]
        #     if not os.path.isdir(save_path):
        #         os.mkdir(save_path)
        #     if not days[date]:
        #         continue
        #     else:
        #         Days[date].to_csv(save_path + '\\2017_' + str(file_num) + ".csv", index = False)

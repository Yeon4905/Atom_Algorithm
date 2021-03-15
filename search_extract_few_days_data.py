# 해당 날짜 찾아 날짜별로 리스트 생성 및 업로드 함수
# 길다고 놀라지 말고 그냥 쭉쭉 내려주세용~
def search_selected_date(df1, days):
    for i in tqdm(range(len(df1)), desc = '한파일라인수'):
        if df1.iloc[i,7] >= 17121600000000 and df1.iloc[i,7] < 17121700000000:
            days[15].append(df1.loc[i])
        elif df1.iloc[i,7] >= 17121700000000 and df1.iloc[i,7] < 17121800000000:
            days[16].append(df1.loc[i])
        elif df1.iloc[i,7] >= 17121800000000 and df1.iloc[i,7] < 17121900000000:
            days[17].append(df1.loc[i])
        elif df1.iloc[i,7] >= 17121900000000 and df1.iloc[i,7] < 17122000000000:
            days[18].append(df1.loc[i])
        elif df1.iloc[i,7] >= 17122000000000 and df1.iloc[i,7] < 17122100000000:
            days[19].append(df1.loc[i])
        elif df1.iloc[i,7] < 17123200000000:
            continue
    return days

# 위의 함수에서 만든 리스트를 데이터프레임 형식으로 바꿔주는 함수
def list_to_dataframe(x, Days):
    for i in tqdm(range(31), desc = '데이터프레임 변환'):
        Days[i] = pd.DataFrame(x[i])
    return Days

# 함수 활용 전체 코드
import pandas as pd
import os
from collections import defaultdict
from tqdm import tqdm
from functools import partial # tqdm progressbar 오류 잡아주는 코드
tqdm = partial(tqdm, position = 0, leave = True )

# 활용 데이터 디렉토리
# 본인이 이용할 데이터 폴더 위치 입력
if __name__ == "__main__":
    dtg_dir = r'\\192.168.0.246\stl_200301\2. 연구용역\2021_(국토부) 교통플랫폼 기반 신규 교통서비스\DTG\DTG_new_16\UTMK_16\\'
    os.chdir(dtg_dir)
    file_list = os.listdir(dtg_dir)
    
    # 원하는 날짜있으면 그것만 입력해서 쓰기
    day_list = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']
    
    # 빈 리스트 한번에 생성하기
    days = [[] for _ in range(31)]
    # days = defaultdict(list)
    Days = ['D' + str(i) for i in range(171201, 171232)]


    # 폴더내 파일 불러오기
    for file in tqdm(file_list, desc = '전체파일리스트'):
        filename, fileExt = os.path.splitext(file)

    # csv 파일 원하는 개수만큼 이용하여 추출하기
    # default는 range(0,400)으로 DTG 데이터 전체 400개임
    # 폴더는 없더라도 일일이 생성하지 않아도 되므로 걱정하지 않아도 됨
    # 0~199까지는 1일부터 15일까지, 200~399는 16일부터 31일 데이터가 존재함
    # 이에 빈 리스트일 때 1kb 파일을 생성하지 않도록 해주는 보드가 마지막 4줄임

    if '.csv' in fileExt:
        for file_num in tqdm(range(200, 201), desc = '파일개수'):
            df = pd.read_csv(dtg_dir + filename[:5] + str(file_num) + '_UTMK' + fileExt, encoding = 'utf-8')
            df = df[:100000]
            search_selected_date(df, days)
            list_to_dataframe(days, Days)
            for date in tqdm(range(31), desc = '1712'):
                save_path = 'C:\\Users\\HYU\\Desktop\\PR\\PR_New1\\1712' + day_list[date]
                if not os.path.isdir(save_path):
                    os.mkdir(save_path)
                if not days[date]:
                    continue
                else:
                    Days[date].to_csv(save_path + '\\2017-' + str(file_num) + ".csv", index = False)

import pandas as pd
import glob
import os
from tqdm import tqdm
from functools import partial  # tqdm progressbar 오류 잡아주는 코드
tqdm = partial(tqdm, position = 0, leave = True )


month = ['171201','171202','171203','171204','171205','171206',
     '171207','171208','171209','171210','171211','171212',
     '171213','171214','171215','171216','171217','171218',
     '171219','171220','171221','171222','171223','171224',
     '171225','171226','171227','171228','171229','171230','171231']

for m in month:
    input_file = "F:\\DTG_new_16\\" + m + "\\"
    output_file = "F:\\DTG_new_16\\merge\\" + m + ".csv"

    allFile_list = glob.glob(os.path.join(input_file))

    allData = []
    
    
    for file in tqdm(allFile_list, desc = 'all'):
        # print(file)
        file_lst = os.listdir(file)
        # print(file_lst)
        for lst in tqdm(file_lst, desc = 'file'):
        
            df = pd.read_csv(file + lst)
            allData.append(df)
    
        dataCombine = pd.concat(allData, axis=0, ignore_index=True)
    
        dataCombine.to_csv(output_file, index=False)

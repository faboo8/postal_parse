
from postal.expand import expand_address
from postal.parser import parse_address
import pandas as pd
from multiprocessing import Pool
import time


df = pd.read_excel('vatidcheck.xlsx', sheet_name=0, header =0, skiprows=[1])
loc_data = df.iloc[:,[3,4,5,6,7,8,9,11,13]]
#loc_concat = loc_data.iloc[:,0].apply(lambda x:  str.cat(loc_data[x]), loc_data.columns[1:])
loc_concat =  loc_data.iloc[:,0]
for col in loc_data.columns[1:]:
    loc_concat = loc_concat.str.cat(list(loc_data[col]), sep = ' ', na_rep='')
    
     
loc_norm = loc_concat.apply(expand_address)
loc_parse = []
loc_raw = []
for address in loc_norm:
    try:
        loc_parse.append(parse_address(address[0]))
        loc_raw.append(address[0])
    except:
        loc_parse.append([('', 'house'), ('', 'unit'),
                         ('', 'po_box'),
                         ('', 'city'),
                         ('', 'postcode'),
                         ('', 'city')])
        loc_raw.append('')
loc_raw=pd.Series(loc_raw)

    
##df2 = pd.DataFrame(loc_pars)
##df2 = pd.DataFrame(loc_pars, columns=['house', 'category','near', 'house_number','road', 'unit','level', 'staircase', 'entrance', 'po_box','postcode','suburb', 'city_district', 'city', 'island', 'state_district', 'state', 'country_region', 'country', 'world_region'])


def create_df(a,b):
    df2 = pd.DataFrame()
    i=a
    for el in loc_parse[a:b]:
        inv_parse = {dict(el)[k] : k for k in dict(el)}
        df2 = pd.concat([df2,pd.DataFrame(inv_parse, index = [i])], join = 'outer', ignore_index=True, axis=0)
        i +=1
    return df2

#multiproccessing
print('start multiprocessing')
t3=time.time()
if __name__ == '__main__':
    pool=Pool(processes=8)
    res = [pool.apply_async(create_df, (a,b)) for (a,b) in
           [(0,7800), (7800,15600), 
           (15600,23400), (23400,31200), (31200,39000),  
           (39000,46800), (46800,54600), (54600,-1)]]
    print(res.get(timeout=1) for r in res)
    
    ret = [re.get() for re in res]
    df_concat =  pd.DataFrame()
    for dataframe in ret:
        df_concat = pd.concat([df_concat,dataframe.loc[:,~dataframe.columns.duplicated()]], join = 'outer', ignore_index=True, axis=0,)
    df_concat= pd.concat([df_concat, loc_raw], axis=1)
t4 = time.time()
print(t4-t3)
writer = pd.ExcelWriter('out.xlsx')
df_concat.to_excel(writer, 'adress_parsed')
loc_raw.to_excel(writer, 'adress_raw')
df.to_excel(writer,'raw_data')
writer.save()


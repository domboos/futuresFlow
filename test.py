from futures_flow.dataProcessing.data_util import read_serialized_dict


if __name__ == '__main__':
    res = read_serialized_dict('regularized_pooled_ols_2010-2020')

    print(res.keys())
